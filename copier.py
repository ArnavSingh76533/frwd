#!/usr/bin/env python3
"""Copy a finite Telegram channel range with album-aware resumable progress.
Python 3.10+. See README.md before running. No live copying occurs on import.
"""
import argparse
import asyncio
import contextlib
import datetime as dt
import json
import math
import os
from pathlib import Path
import signal
import sqlite3
import sys
import time

SOURCE = -1003571991185
DESTINATION = -1004455533802
START = 4
END = 56521
JOB = {"source": SOURCE, "destination": DESTINATION, "start": START, "end": END}
STOP = False


class Halt(Exception):
    pass


class Limited(Halt):
    def __init__(self, seconds):
        self.seconds = max(1, int(seconds))
        super().__init__(f"Telegram rate limit: wait at least {self.seconds} seconds.")


def log(message):
    print(f"[{dt.datetime.now().astimezone().isoformat(timespec='seconds')}] {message}", flush=True)


def stop_signal(*_):
    global STOP
    STOP = True
    log("Stop requested; finishing the current operation and saving progress.")


def atomic_json(path, value):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


@contextlib.contextmanager
def exclusive_lock(path):
    # Kernel locks are released even after a crash. Never delete the lock file.
    f = path.open("a+b")
    try:
        if os.name == "nt":
            import msvcrt
            f.seek(0)
            f.write(b"0")
            f.flush()
            f.seek(0)
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                raise Halt("Another copier is using this state directory.") from None
        else:
            import fcntl
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise Halt("Another copier is using this state directory.") from None
        yield
    finally:
        f.close()


class State:
    def __init__(self, directory, readonly=False):
        self.directory = Path(directory)
        self.txt = self.directory / "msg.txt"
        if readonly:
            self.db = sqlite3.connect((self.directory / "progress.sqlite3").as_uri() + "?mode=ro", uri=True)
            self.db.row_factory = sqlite3.Row
            self.db.execute("BEGIN")
            if self.get("job") != JOB:
                raise Halt("Database belongs to a different job or is not initialized yet.")
            return
        self.db = sqlite3.connect(self.directory / "progress.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY, group_id TEXT, kind TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending');
            CREATE INDEX IF NOT EXISTS album_idx ON messages(group_id);
            CREATE TABLE IF NOT EXISTS operations (
                seq INTEGER PRIMARY KEY, source_ids TEXT NOT NULL, through_id INTEGER NOT NULL,
                state TEXT NOT NULL, destination_ids TEXT, detail TEXT,
                created_at REAL NOT NULL, finished_at REAL);
        ''')
        if self.get("job") is None:
            restored = {}
            if self.txt.exists():
                try:
                    restored = json.loads(self.txt.read_text(encoding="utf-8"))
                except (ValueError, OSError):
                    raise Halt("msg.txt is invalid. Restore a valid backup; refusing to restart from 4.")
                if not isinstance(restored, dict) or restored.get("job") != JOB:
                    raise Halt("msg.txt belongs to another job or has an unsupported format.")
                if restored.get("unresolved_operation"):
                    raise Halt("msg.txt records an unresolved send. Restore the SQLite database to resolve it.")
            last = restored.get("last_processed_id", START - 1)
            if type(last) is not int or not START - 1 <= last <= END:
                raise Halt("Invalid checkpoint in msg.txt.")
            with self.db:
                for key, value in {"job": JOB, "last": last, "scan": last,
                                   "copied": restored.get("copied_messages", 0),
                                   "skipped": restored.get("skipped_ids", 0),
                                   "not_before": restored.get("not_before_epoch", 0)}.items():
                    self.set(key, value)
        elif self.get("job") != JOB:
            raise Halt("Database belongs to a different source/destination/range.")
        self.mirror()

    def get(self, key, default=None):
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, json.dumps(value)))

    def unresolved(self):
        return self.db.execute("SELECT * FROM operations WHERE state IN ('sending','uncertain','partial') ORDER BY seq LIMIT 1").fetchone()

    def mirror(self):
        pending = self.unresolved()
        atomic_json(self.txt, {
            "version": 1, "job": JOB, "last_processed_id": self.get("last"),
            "next_id": min(END + 1, self.get("last") + 1),
            "remaining_ids": max(0, END - self.get("last")),
            "scanned_through_id": self.get("scan"),
            "copied_messages": self.get("copied"), "skipped_ids": self.get("skipped"),
            "not_before_epoch": self.get("not_before", 0),
            "unresolved_operation": dict(pending) if pending else None,
        })

    def cooldown(self, seconds, padding=0):
        with self.db:
            self.set("not_before", max(self.get("not_before", 0), time.time() + seconds + padding))
        self.mirror()

    def status(self, seconds=3.0):
        last = self.get("last")
        counts = dict(self.db.execute("SELECT kind, count(*) FROM messages WHERE id>? GROUP BY kind", (last,)))
        result = {
            "source": SOURCE, "destination": DESTINATION, "range": f"{START}..{END}",
            "total_id_slots": END - START + 1, "last_processed_id": last,
            "remaining_id_slots": END - last, "copied_messages": self.get("copied"),
            "skipped_ids": self.get("skipped"), "scanned_through_id": self.get("scan"),
            "remaining_scanned_by_kind": counts,
            "cooldown_remaining_seconds": max(0, math.ceil(self.get("not_before", 0) - time.time())),
            "unresolved_operation": dict(self.unresolved()) if self.unresolved() else None,
        }
        if self.get("scan") == END:
            result["estimated_pacing_hours_remaining"] = round(counts.get("copy", 0) * seconds / 3600, 2)
        print(json.dumps(result, indent=2), flush=True)

    def record_scan(self, rows, through):
        with self.db:
            self.db.executemany("INSERT OR IGNORE INTO messages(id,group_id,kind) VALUES (?,?,?)", rows)
            self.set("scan", through)
        self.mirror()

    def skip(self, through):
        last = self.get("last")
        with self.db:
            self.db.execute("UPDATE messages SET state='skipped' WHERE id>? AND id<=?", (last, through))
            self.set("skipped", self.get("skipped") + through - last)
            self.set("last", through)
        self.mirror()

    def prepare(self, ids, through):
        with self.db:
            cur = self.db.execute("INSERT INTO operations(source_ids,through_id,state,created_at) VALUES (?,?,'sending',?)",
                                  (json.dumps(ids), through, time.time()))
        self.mirror()  # Pending send is durable before the HTTP request.
        return cur.lastrowid

    def mark(self, seq, state, detail, destinations=None):
        with self.db:
            self.db.execute("UPDATE operations SET state=?,detail=?,destination_ids=? WHERE seq=?",
                            (state, detail, json.dumps(destinations) if destinations is not None else None, seq))
        self.mirror()

    def finish(self, seq, destinations, detail="confirmed"):
        row = self.db.execute("SELECT * FROM operations WHERE seq=?", (seq,)).fetchone()
        ids = json.loads(row["source_ids"])
        last, through = self.get("last"), row["through_id"]
        count = len(destinations)
        if not 0 <= count <= len(ids) or through <= last:
            raise Halt("Invalid operation result; database was not advanced.")
        with self.db:
            self.db.execute("UPDATE operations SET state='done',destination_ids=?,detail=?,finished_at=? WHERE seq=?",
                            (json.dumps(destinations), detail, time.time(), seq))
            self.db.execute("UPDATE messages SET state='processed' WHERE id>? AND id<=?", (last, through))
            self.set("copied", self.get("copied") + count)
            self.set("skipped", self.get("skipped") + through - last - count)
            self.set("last", through)
        self.mirror()


def load_env(path):
    # Deliberately no shell expansion; secrets never appear in command substitution.
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('\"').strip("'"))


class BotAPI:
    def __init__(self, token):
        import requests
        self.requests = requests
        self.session = requests.Session()  # No automatic retries on non-idempotent sends.
        self.base = f"https://api.telegram.org/bot{token}"

    def call(self, method, payload):
        try:
            response = self.session.post(f"{self.base}/{method}", json=payload, timeout=(15, 120))
            data = response.json()
        except (self.requests.RequestException, ValueError):
            # requests exceptions may contain the token in their URL: never print them.
            raise Halt("Network/response failure; the request outcome may be unknown.") from None
        if not isinstance(data, dict):
            raise Halt("Malformed Telegram response; request outcome unknown.")
        if response.status_code == 429 or data.get("error_code") == 429:
            raise Limited(data.get("parameters", {}).get("retry_after", 60))
        if not data.get("ok"):
            code = data.get("error_code", response.status_code)
            description = str(data.get("description", "Unknown Telegram error"))
            if 400 <= code < 500:
                raise Rejected(code, description)
            raise Halt(f"Telegram server failure ({code}); request outcome may be unknown.")
        if "result" not in data:
            raise Halt("Telegram omitted result; request outcome unknown.")
        return data["result"]

    def preflight(self):
        me = self.call("getMe", {})
        for chat_id in (SOURCE, DESTINATION):
            chat = self.call("getChat", {"chat_id": chat_id})
            member = self.call("getChatMember", {"chat_id": chat_id, "user_id": me["id"]})
            if chat.get("type") != "channel":
                raise Halt(f"{chat_id} must be a channel for this configured job.")
            if member.get("status") not in ("administrator", "creator"):
                raise Halt(f"Add the bot as administrator of channel {chat_id}.")
            if chat_id == SOURCE and chat.get("has_protected_content"):
                raise Halt("Source has content protection enabled; copying is not attempted.")
            if chat_id == DESTINATION and not member.get("can_post_messages", member.get("status") == "creator"):
                raise Halt("Bot needs Post Messages permission in the destination.")
        log(f"Bot @{me.get('username')} verified in both channels.")
        return me["id"]


class Rejected(Halt):
    def __init__(self, code, description):
        self.code = code
        super().__init__(f"Telegram rejected request ({code}): {description}")


def classify(message):
    from telethon import types
    if message is None or isinstance(message, types.MessageEmpty):
        return "missing"
    if isinstance(message, types.MessageService):
        return "service"
    if getattr(message, "noforwards", False):
        return "protected"
    return "copy"


async def scan(state, token, api_id, api_hash, bot_id, scan_delay):
    from telethon import TelegramClient, errors, functions, types
    client = TelegramClient(str(state.directory / "bot"), api_id, api_hash,
                            flood_sleep_threshold=0, request_retries=0,
                            connection_retries=3, raise_last_call_error=True)
    try:
        await client.start(bot_token=token)
        me = await client.get_me()
        if not me.bot or me.id != bot_id:
            raise Halt("bot.session belongs to a different account. Use the correct bot session.")
        # Telegram explicitly permits bots to use a zero access hash for uncached peers.
        channel = types.InputChannel(3571991185, 0)
        info = await client(functions.channels.GetChannelsRequest([channel]))
        source = next((c for c in info.chats if c.id == 3571991185), None)
        if source is None or isinstance(source, types.ChannelForbidden):
            raise Halt("The bot cannot read the source channel.")
        if getattr(source, "noforwards", False):
            raise Halt("Source is protected; copying is not attempted.")
        channel = types.InputChannel(source.id, getattr(source, "access_hash", 0) or 0)
        while state.get("scan") < END and not STOP:
            first = state.get("scan") + 1
            last = min(END, first + 99)
            result = await client(functions.channels.GetMessagesRequest(
                channel, [types.InputMessageID(i) for i in range(first, last + 1)]))
            if not hasattr(result, "messages"):
                raise Halt("Unexpected metadata response; scan checkpoint was not advanced.")
            by_id = {m.id: m for m in result.messages}
            rows = []
            for i in range(first, last + 1):
                m = by_id.get(i)
                group = getattr(m, "grouped_id", None)
                rows.append((i, str(group) if group is not None else None, classify(m)))
            state.record_scan(rows, last)
            if last == END or (last - START + 1) % 1000 == 0:
                log(f"Scanned through ID {last}; {END - last:,} IDs left to inspect.")
            await asyncio.sleep(scan_delay)
    except errors.FloodWaitError as e:
        raise Limited(e.seconds) from None
    except errors.RPCError as e:
        # Only the type is logged, avoiding authentication/session details.
        raise Halt(f"Metadata scan stopped: {type(e).__name__}. Check bot access and credentials.") from None
    finally:
        await client.disconnect()


def next_unit(state):
    first = state.get("last") + 1
    row = state.db.execute("SELECT * FROM messages WHERE id=?", (first,)).fetchone()
    if row is None:
        raise Halt("Metadata is incomplete; run the scan before copying.")
    if row["kind"] != "copy":
        later = state.db.execute("SELECT min(id) FROM messages WHERE id>? AND kind='copy'", (first,)).fetchone()[0]
        return [], (later - 1 if later else END)
    if row["group_id"] is None:
        return [first], first
    # Global manifest prevents splitting an album at a 100-ID scan boundary.
    album = list(state.db.execute("SELECT * FROM messages WHERE group_id=? ORDER BY id", (row["group_id"],)))
    if album[0]["id"] < first:
        raise Halt("Checkpoint falls inside an album. Restore an album-boundary checkpoint.")
    end = album[-1]["id"]
    span = list(state.db.execute("SELECT * FROM messages WHERE id>=? AND id<=?", (first, end)))
    if any(r["kind"] == "copy" and r["group_id"] != row["group_id"] for r in span):
        raise Halt("Interleaved album detected; stopped to avoid changing post order.")
    if any(r["kind"] not in ("copy", "missing") for r in span):
        raise Halt("Album contains protected/unsupported entries; stopped for review.")
    ids = [r["id"] for r in album if r["kind"] == "copy"]
    if len(ids) > 100:
        raise Halt("Album exceeds copyMessages batch limit.")
    return ids, end


def destination_ids(result):
    if not isinstance(result, list) or any(not isinstance(x, dict) or type(x.get("message_id")) is not int for x in result):
        raise Halt("Malformed copy result; review the pending operation.")
    ids = [x["message_id"] for x in result]
    if any(x <= 0 for x in ids) or len(set(ids)) != len(ids):
        raise Halt("Invalid destination IDs; review the pending operation.")
    return ids


def send_unit(state, api, ids, through, seconds):
    seq = state.prepare(ids, through)
    try:
        result = api.call("copyMessages", {
            "chat_id": DESTINATION, "from_chat_id": SOURCE, "message_ids": ids,
            "disable_notification": True, "remove_caption": False,
        })
        dest = destination_ids(result)
    except Limited as e:
        state.cooldown(e.seconds, padding=5)
        state.mark(seq, "rejected", "Rate limited; no progress advanced.")
        raise
    except Rejected as e:
        # Unknown 4xx errors are NOT converted into missing-message skips.
        state.mark(seq, "rejected", str(e))
        raise
    except Halt as e:
        state.mark(seq, "uncertain", str(e))
        raise
    # Persist the pacing deadline before accepting the result, including partials.
    state.cooldown(seconds * max(1, len(dest)))
    if len(dest) != len(ids):
        state.mark(seq, "partial", "Telegram skipped one or more requested entries; review destination.", dest)
        raise Halt("Partial copy result saved. Inspect --status, then use --resolve accept-partial if appropriate.")
    state.finish(seq, dest)
    log(f"Copied {len(dest)} message(s), through ID {through}; {END - through:,} ID slots left.")


def resolve(state, action, supplied):
    row = state.unresolved()
    if row is None:
        raise Halt("No unresolved operation.")
    if action == "retry":
        state.mark(row["seq"], "retry_authorized", "Operator checked destination and requested retry.")
        log("Retry enabled. Run again without --resolve. Duplicate risk if the earlier send succeeded.")
    elif action == "done":
        try:
            ids = [int(v) for v in supplied.split(",") if v.strip()]
        except ValueError:
            raise Halt("--destination-ids must be comma-separated integers.") from None
        expected = len(json.loads(row["source_ids"]))
        if len(ids) != expected or len(set(ids)) != expected or any(i <= 0 for i in ids):
            raise Halt(f"Provide exactly {expected} actual destination message IDs using --destination-ids.")
        state.finish(row["seq"], ids, "Operator confirmed complete delivery.")
    elif action == "accept-partial":
        if row["state"] != "partial" or row["destination_ids"] is None:
            raise Halt("This operation has no confirmed partial result to accept.")
        state.finish(row["seq"], json.loads(row["destination_ids"]), "Operator accepted partial delivery; omitted items counted as skipped.")
    state.status()


def wait_until(deadline):
    while time.time() < deadline and not STOP:
        time.sleep(min(1, max(0, deadline - time.time())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", default=str(Path(__file__).resolve().parent / "state"))
    parser.add_argument("--status", action="store_true", help="Show local progress without Telegram calls")
    parser.add_argument("--scan-only", action="store_true", help="Inspect source metadata without copying")
    parser.add_argument("--seconds-per-message", type=float, default=3.0)
    parser.add_argument("--scan-delay", type=float, default=1.0)
    parser.add_argument("--resolve", choices=("retry", "done", "accept-partial"))
    parser.add_argument("--destination-ids", default="")
    args = parser.parse_args()
    if not math.isfinite(args.seconds_per_message) or args.seconds_per_message < 1.1:
        parser.error("--seconds-per-message must be at least 1.1 (3.0 recommended)")
    if not math.isfinite(args.scan_delay) or args.scan_delay < 0.5:
        parser.error("--scan-delay must be at least 0.5")
    os.umask(0o077)
    directory = Path(args.state_dir).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if args.status and (directory / "progress.sqlite3").exists():
        state = State(directory, readonly=True)
        try:
            state.status(args.seconds_per_message)
        finally:
            state.db.close()
        return 0
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop_signal)
    with exclusive_lock(directory / "copier.lock"):
        state = State(directory)
        try:
            if args.status:
                state.status(args.seconds_per_message)
                return 0
            if args.resolve:
                resolve(state, args.resolve, args.destination_ids)
                return 0
            if state.unresolved():
                state.status(args.seconds_per_message)
                raise Halt("Unresolved send found. Inspect the destination before using --resolve.")
            if state.get("last") >= END:
                log("Range complete.")
                state.status(args.seconds_per_message)
                return 0
            remaining = math.ceil(state.get("not_before", 0) - time.time())
            if remaining > 0:
                raise Halt(f"Saved cooldown is active for {remaining}s; restart after it expires.")
            load_env(Path(__file__).resolve().parent / ".env")
            token = os.environ.get("BOT_TOKEN", "").strip()
            if not token or ":" not in token:
                raise Halt("Set BOT_TOKEN in .env.")
            api = BotAPI(token)
            try:
                bot_id = api.preflight()
                if state.get("scan") < END:
                    try:
                        api_id = int(os.environ.get("API_ID", "0"))
                    except ValueError:
                        api_id = 0
                    api_hash = os.environ.get("API_HASH", "").strip()
                    if api_id <= 0 or not api_hash:
                        raise Halt("Set API_ID and API_HASH in .env for album metadata scanning.")
                    log("Scanning IDs to identify complete albums, missing entries and service messages.")
                    asyncio.run(scan(state, token, api_id, api_hash, bot_id, args.scan_delay))
                state.status(args.seconds_per_message)
                if args.scan_only or STOP:
                    return 0
                if state.get("copied") == 0 and not state.db.execute("SELECT 1 FROM messages WHERE kind='copy' AND id>? LIMIT 1", (state.get("last"),)).fetchone():
                    raise Halt("No copyable messages found. Verify access before accepting an empty source range.")
                while state.get("last") < END and not STOP:
                    ids, through = next_unit(state)
                    if not ids:
                        log(f"Skipping absent/service/protected IDs through {through}.")
                        state.skip(through)
                        continue
                    send_unit(state, api, ids, through, args.seconds_per_message)
                    wait_until(state.get("not_before"))
                log("Stopped with progress saved." if STOP else "Range complete.")
                state.status(args.seconds_per_message)
                return 0
            except Limited as e:
                state.cooldown(e.seconds, padding=5)
                raise Halt(f"{e} Stopped. Cooldown saved; restart later.") from None
        finally:
            state.db.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Halt as e:
        log(str(e))
        sys.exit(2)
    except Exception as e:
        # No URLs, tokens, request dumps or full session tracebacks in logs.
        log(f"Stopped on {type(e).__name__}. Progress retained. Use --status to inspect pending sends.")
        sys.exit(1)

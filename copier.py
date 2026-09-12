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
import re
import secrets
import tempfile
from urllib.parse import urlparse
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
TRANSFER_MODE = "copy"


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
                                   "mode": restored.get("transfer_mode", "copy"),
                                   "not_before": restored.get("not_before_epoch", 0)}.items():
                    self.set(key, value)
        elif self.get("job") != JOB:
            raise Halt("Database belongs to a different source/destination/range.")
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(operations)")}
        if "transport" not in columns:
            self.db.execute("ALTER TABLE operations ADD COLUMN transport TEXT DEFAULT 'copy'")
        if "random_ids" not in columns:
            self.db.execute("ALTER TABLE operations ADD COLUMN random_ids TEXT")
        self.db.commit()
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
            "transfer_mode": self.get("mode", "copy"),
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
            "transfer_mode": self.get("mode", "copy"),
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

    def prepare(self, ids, through, transport="copy", random_ids=None):
        with self.db:
            cur = self.db.execute("INSERT INTO operations(source_ids,through_id,state,created_at,transport,random_ids) VALUES (?,?,'sending',?,?,?)",
                                  (json.dumps(ids), through, time.time(), transport, json.dumps(random_ids)))
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
        source_protected = False
        for chat_id in (SOURCE, DESTINATION):
            chat = self.call("getChat", {"chat_id": chat_id})
            member = self.call("getChatMember", {"chat_id": chat_id, "user_id": me["id"]})
            if chat.get("type") != "channel":
                raise Halt(f"{chat_id} must be a channel for this configured job.")
            if member.get("status") not in ("administrator", "creator"):
                raise Halt(f"Add the bot as administrator of channel {chat_id}.")
            if chat_id == SOURCE and chat.get("has_protected_content"):
                source_protected = True
            if chat_id == DESTINATION and not member.get("can_post_messages", member.get("status") == "creator"):
                raise Halt("Bot needs Post Messages permission in the destination.")
        log(f"Bot @{me.get('username')} verified in both channels.")
        return me["id"], source_protected


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


async def fetch_messages(client, channel, ids):
    from telethon import functions, types
    result = await client(functions.channels.GetMessagesRequest(
        channel, [types.InputMessageID(i) for i in ids]))
    if not hasattr(result, "messages"):
        raise Halt("Unexpected metadata response; checkpoint was not advanced.")
    return {m.id: m for m in result.messages}


async def scan(state, client, channel, scan_delay):
    while state.get("scan") < END and not STOP:
        first = state.get("scan") + 1
        last = min(END, first + 99)
        by_id = await fetch_messages(client, channel, range(first, last + 1))
        rows = []
        for i in range(first, last + 1):
            m = by_id.get(i)
            group = getattr(m, "grouped_id", None)
            kind = classify(m)
            if kind == "protected" and TRANSFER_MODE == "upload":
                kind = "copy"
            rows.append((i, str(group) if group is not None else None, kind))
        state.record_scan(rows, last)
        if last == END or (last - START + 1) % 1000 == 0:
            log(f"Scanned through ID {last}; {END - last:,} IDs left to inspect.")
        await asyncio.sleep(scan_delay)


def next_unit(state):
    first = state.get("last") + 1
    row = state.db.execute("SELECT * FROM messages WHERE id=?", (first,)).fetchone()
    if row is None:
        raise Halt("Metadata is incomplete; run the scan before copying.")
    if row["kind"] != "copy":
        later = state.db.execute("SELECT min(id) FROM messages WHERE id>? AND kind IN ('copy','protected')", (first,)).fetchone()[0]
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


def parse_reference(value):
    """Return (optional source channel, ID) without accepting topics or other hosts."""
    value = str(value).strip()
    if value.isdecimal() and int(value) > 0:
        return None, int(value)
    if value.startswith('t.me/'):
        value = 'https://' + value
    url = urlparse(value)
    if url.scheme not in ('http', 'https') or url.netloc.lower() not in ('t.me', 'www.t.me', 'telegram.me'):
        raise Halt('Enter a positive message ID or a Telegram message link.')
    match = re.fullmatch(r'/c/(\d+)/(\d+)/?', url.path)
    if match:
        chat, msg = map(int, match.groups())
        if chat > 0 and msg > 0:
            return -(1_000_000_000_000 + chat), msg
    match = re.fullmatch(r'/([A-Za-z][A-Za-z0-9_]{3,31})/(\d+)/?', url.path)
    if match and int(match[2]) > 0:
        return '@' + match[1], int(match[2])
    raise Halt('Expected a channel message link such as https://t.me/c/3571991185/4 (topic links are not supported).')


def read_saved_job(directory):
    database = directory / 'progress.sqlite3'
    if database.exists():
        with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True) as db:
            try:
                row = db.execute("SELECT value FROM settings WHERE key='job'").fetchone()
            except sqlite3.OperationalError:
                return None
            if row:
                return json.loads(row[0])
    checkpoint = directory / 'msg.txt'
    if checkpoint.exists():
        try:
            return json.loads(checkpoint.read_text()).get('job')
        except (ValueError, AttributeError):
            raise Halt('Invalid msg.txt; repair or restore the checkpoint before proceeding.') from None
    return None


def configure_job(args, api):
    global SOURCE, DESTINATION, START, END, JOB
    root = Path(args.state_dir or Path(__file__).resolve().parent / 'state').resolve()
    directory = root
    if not args.state_dir and (root / 'active_job.json').exists():
        try:
            directory = (root / json.loads((root / 'active_job.json').read_text())['directory']).resolve()
        except (ValueError, KeyError, TypeError):
            raise Halt('Invalid active_job.json; specify --state-dir explicitly.') from None
        if not directory.is_relative_to(root):
            raise Halt('Invalid active job directory.')
    saved = read_saved_job(directory)
    defaults = saved or JOB
    administrative = args.status or args.resolve or args.resume
    start_ref, end_ref = args.from_message, args.to_message
    if not administrative and sys.stdin.isatty():
        channel_number = -defaults['source'] - 1_000_000_000_000
        if start_ref is None:
            start_ref = input(f"First message link or ID [https://t.me/c/{channel_number}/{defaults['start']}]: ").strip() or str(defaults['start'])
        if end_ref is None:
            end_ref = input(f"Last message link or ID [https://t.me/c/{channel_number}/{defaults['end']}]: ").strip() or str(defaults['end'])
    if (start_ref is None) != (end_ref is None):
        raise Halt('Supply both --from and --to, or use the interactive prompts.')
    if not administrative and start_ref is None:
        raise Halt('No terminal input. Supply --from and --to, or use --resume for the saved job.')
    a_chat, first = parse_reference(start_ref or defaults['start'])
    b_chat, last = parse_reference(end_ref or defaults['end'])
    def resolve_chat(chat):
        if isinstance(chat, str):
            return api.call('getChat', {'chat_id': chat})['id']
        return chat
    a_chat, b_chat = resolve_chat(a_chat), resolve_chat(b_chat)
    if a_chat is not None and b_chat is not None and a_chat != b_chat:
        raise Halt('The first and last links must belong to the same source channel.')
    source = a_chat or b_chat or defaults['source']
    destination = args.destination or defaults['destination']
    if not first <= last <= 2_147_483_647:
        raise Halt('The end ID must be at least the start ID and fit a Telegram message ID.')
    if source == destination or source >= -1_000_000_000_000 or destination >= -1_000_000_000_000:
        raise Halt('Use two different channel IDs (in -100... format).')
    selected = dict(source=source, destination=destination, start=first, end=last)
    if selected != saved and not args.state_dir:
        key = f'{-source}_{-destination}_{first}_{last}'
        directory = root / 'jobs' / key
    SOURCE, DESTINATION, START, END, JOB = source, destination, first, last, selected
    directory.mkdir(parents=True, exist_ok=True)
    # Administrative reads don't change which interactive job is active.
    if not args.state_dir and not args.status and not args.resolve:
        root.mkdir(parents=True, exist_ok=True)
        atomic_json(root / 'active_job.json', {'directory': str(directory.relative_to(root))})
    return directory


def enable_upload(state, requested, reason):
    global TRANSFER_MODE
    if requested == 'copy':
        raise Halt(f'{reason} Run with --mode upload to download and upload instead.')
    if requested != 'upload' and state.get('mode') != 'upload':
        if not sys.stdin.isatty():
            raise Halt(f'{reason} Use --mode upload to select the download/upload fallback.')
        answer = input(f'{reason}\nDownload and upload this range instead? [Y/n]: ').strip().lower()
        if answer not in ('', 'y', 'yes'):
            raise Halt('Transfer cancelled; progress retained.')
    TRANSFER_MODE = 'upload'
    with state.db:
        state.set('mode', 'upload')
        state.db.execute("UPDATE messages SET kind='copy' WHERE id>? AND kind='protected'", (state.get('last'),))
    state.mirror()
    log('Download/upload mode enabled. Albums and available original thumbnails will be carried over.')


async def send_upload_unit(state, client, source, destination, ids, through, seconds):
    from telethon import errors
    import media_upload as upload
    by_id = await fetch_messages(client, source, ids)
    messages = []
    for source_id in ids:
        message = by_id.get(source_id)
        if classify(message) in ('missing', 'service'):
            # Refresh disappeared entries and re-plan before posting anything.
            with state.db:
                state.db.execute('UPDATE messages SET kind=?,group_id=NULL WHERE id=?', (classify(message), source_id))
            state.mirror()
        else:
            messages.append(message)
    if len(messages) != len(ids):
        log('Source changed after scan; missing entries refreshed. Replanning the next unit.')
        return
    upload.validate_album(messages)
    temp_root = state.directory / 'transfers'
    temp_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='unit-', dir=temp_root) as temp:
        upload.check_space(messages, temp)
        stager = upload.Stager(client, destination, temp, log, stopped=lambda: STOP)
        staged = [await stager.stage(message) for message in messages]
        stager.check_stop()
        random_ids = [secrets.randbits(63) or 1 for _ in ids]
        request = upload.build_request(destination, messages, staged, random_ids)
        seq = state.prepare(ids, through, transport='upload', random_ids=random_ids)
        try:
            result = await client(request)
            dest = upload.extract_ids(result, random_ids)
        except errors.FloodWaitError as e:
            state.cooldown(e.seconds, padding=5)
            state.mark(seq, 'rejected', 'Upload send was rate limited; no progress advanced.')
            raise Limited(e.seconds) from None
        except errors.RPCError as e:
            # A rejected 4xx RPC doesn't imply successful delivery. Server failures may.
            state.mark(seq, 'rejected' if 400 <= e.code < 500 else 'uncertain', type(e).__name__)
            raise Halt(f'Upload send stopped: {type(e).__name__}. Progress was not advanced.') from None
        except Exception as e:
            state.mark(seq, 'uncertain', f'Upload outcome must be reviewed ({type(e).__name__}).')
            raise Halt('Upload response was interrupted or incomplete. Inspect --status before resolving the send.') from None
        state.cooldown(seconds * len(dest))
        state.finish(seq, dest, 'Upload confirmed with MTProto random-ID mapping.')
        log(f'Uploaded {len(dest)} message(s), through ID {through}; {END - through:,} ID slots left.')


def create_client(api_id, api_hash):
    from telethon import TelegramClient
    from telethon.sessions import MemorySession
    class TransferClient(TelegramClient):
        # Telethon 1.45's normal login calls GetDifference even when update
        # delivery is disabled. This finite job must not consume that stream.
        # These two version-pinned hooks suppress synchronization and dispatch;
        # authorization itself still uses Telethon's normal bot sign-in.
        async def _on_login(self, user):
            self._mb_entity_cache.set_self_user(user.id, user.bot, user.access_hash)
            self._authorized = True
            return user

        async def _update_loop(self):
            return

    # A fresh authorization key per run prevents accidental key sharing across hosts.
    # No update stream, polling, catch-up, webhook changes or bot command handlers.
    return TransferClient(MemorySession(), api_id, api_hash, receive_updates=False, catch_up=False,
                          flood_sleep_threshold=0, request_retries=0, connection_retries=3,
                          raise_last_call_error=True)


async def transfer(state, api, token, api_id, api_hash, bot_id, args):
    from telethon import errors, functions, types
    import media_upload as upload
    client = create_client(api_id, api_hash)
    try:
        await client.start(bot_token=token)
        me = await client.get_me()
        if not me.bot or me.id != bot_id:
            raise Halt('Telegram authorization does not match BOT_TOKEN.')
        channel_number = -SOURCE - 1_000_000_000_000
        dest_number = -DESTINATION - 1_000_000_000_000
        info = await client(functions.channels.GetChannelsRequest([
            types.InputChannel(channel_number, 0), types.InputChannel(dest_number, 0)]))
        entities = {chat.id: chat for chat in info.chats}
        source, destination = entities.get(channel_number), entities.get(dest_number)
        if source is None or destination is None or any(isinstance(c, types.ChannelForbidden) for c in (source, destination)):
            raise Halt('The bot cannot access both channels.')
        if getattr(source, 'noforwards', False) and TRANSFER_MODE != 'upload':
            enable_upload(state, args.mode, 'Source has content protection enabled.')
        source = types.InputChannel(channel_number, getattr(source, 'access_hash', 0) or 0)
        destination = types.InputPeerChannel(dest_number, getattr(destination, 'access_hash', 0) or 0)
        if state.get('scan') < END:
            log('Scanning message IDs and album membership before transfer.')
            await scan(state, client, source, args.scan_delay)
        state.status(args.seconds_per_message)
        if args.scan_only or STOP:
            return
        if TRANSFER_MODE != 'upload' and state.db.execute("SELECT 1 FROM messages WHERE id>? AND kind='protected' LIMIT 1", (state.get('last'),)).fetchone():
            enable_upload(state, args.mode, 'The selected range contains protected messages.')
        if state.get('copied') == 0 and not state.db.execute("SELECT 1 FROM messages WHERE kind IN ('copy','protected') AND id>? LIMIT 1", (state.get('last'),)).fetchone():
            raise Halt('No transferable messages found. Check the range and source access.')
        while state.get('last') < END and not STOP:
            first = state.db.execute('SELECT kind FROM messages WHERE id=?', (state.get('last') + 1,)).fetchone()
            if first and first[0] == 'protected':
                enable_upload(state, args.mode, 'This source message has content protection enabled.')
            ids, through = next_unit(state)
            if not ids:
                log(f'Skipping missing/service IDs through {through}.')
                state.skip(through)
                continue
            if TRANSFER_MODE == 'upload':
                await send_upload_unit(state, client, source, destination, ids, through, args.seconds_per_message)
            else:
                try:
                    send_unit(state, api, ids, through, args.seconds_per_message)
                except Rejected as e:
                    if e.code == 400 and any(term in str(e).lower() for term in ('protected', 'forwards_restricted')):
                        enable_upload(state, args.mode, 'Telegram rejected copying because of content protection.')
                        continue
                    raise
            while time.time() < state.get('not_before') and not STOP:
                await asyncio.sleep(min(1, max(0, state.get('not_before') - time.time())))
        log('Stopped with progress saved.' if STOP else 'Range complete.')
        state.status(args.seconds_per_message)
    except errors.FloodWaitError as e:
        raise Limited(e.seconds) from None
    except upload.UploadStopped as e:
        log(str(e))
    except upload.UploadProblem as e:
        raise Halt(str(e)) from None
    except errors.RPCError as e:
        raise Halt(f'Telegram stopped the transfer: {type(e).__name__}. Current progress retained.') from None
    finally:
        await client.disconnect()


def main():
    global TRANSFER_MODE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', default=None, help='Explicit state directory; otherwise remember the selected job')
    parser.add_argument('--from', dest='from_message', help='First source message link or numeric ID')
    parser.add_argument('--to', dest='to_message', help='Last source message link or numeric ID (inclusive)')
    parser.add_argument('--destination', type=int, help='Destination channel ID; defaults to the saved job')
    parser.add_argument('--resume', action='store_true', help='Resume the selected job without range prompts')
    parser.add_argument('--mode', choices=('ask', 'copy', 'upload'), default='ask')
    parser.add_argument('--status', action='store_true', help='Show local progress without Telegram calls')
    parser.add_argument('--scan-only', action='store_true', help='Inspect source metadata without posting')
    parser.add_argument('--seconds-per-message', type=float, default=3.0)
    parser.add_argument('--scan-delay', type=float, default=1.0)
    parser.add_argument('--resolve', choices=('retry', 'done', 'accept-partial'))
    parser.add_argument('--destination-ids', default='')
    args = parser.parse_args()
    if not math.isfinite(args.seconds_per_message) or args.seconds_per_message < 1.1:
        parser.error('--seconds-per-message must be at least 1.1 (3.0 recommended)')
    if not math.isfinite(args.scan_delay) or args.scan_delay < 0.5:
        parser.error('--scan-delay must be at least 0.5')
    if (args.status or args.resolve or args.resume) and (args.from_message or args.to_message):
        parser.error('--status, --resolve and --resume use the saved range; omit --from/--to')
    os.umask(0o077)
    load_env(Path(__file__).resolve().parent / '.env')
    token = os.environ.get('BOT_TOKEN', '').strip()
    api = BotAPI(token)  # Local construction only; prompts happen before network calls.
    directory = configure_job(args, api)
    if args.status and (directory / 'progress.sqlite3').exists():
        state = State(directory, readonly=True)
        try:
            state.status(args.seconds_per_message)
        finally:
            state.db.close()
        return 0
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop_signal)
    with exclusive_lock(directory / 'copier.lock'):
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
                raise Halt('Unresolved send found. Inspect the destination before using --resolve.')
            if state.get('last') >= END:
                log('Range complete.')
                state.status(args.seconds_per_message)
                return 0
            remaining = math.ceil(state.get('not_before', 0) - time.time())
            if remaining > 0:
                raise Halt(f'Saved cooldown active for {remaining}s; restart after it expires.')
            if not token or ':' not in token:
                raise Halt('Set BOT_TOKEN in .env.')
            try:
                api_id = int(os.environ.get('API_ID', '0'))
            except ValueError:
                api_id = 0
            api_hash = os.environ.get('API_HASH', '').strip()
            if api_id <= 0 or not api_hash:
                raise Halt('Set API_ID and API_HASH in .env for Telegram metadata and uploads.')
            try:
                bot_id, protected = api.preflight()
                TRANSFER_MODE = 'copy'
                if args.mode == 'upload' or (args.mode == 'ask' and state.get('mode') == 'upload') or protected:
                    enable_upload(state, args.mode, 'Source has content protection enabled.' if protected else 'Upload mode selected.')
                else:
                    with state.db:
                        state.set('mode', 'copy')
                log(f'Source {SOURCE} → destination {DESTINATION}; IDs {START}..{END}. State: {directory}')
                asyncio.run(transfer(state, api, token, api_id, api_hash, bot_id, args))
                return 0
            except Limited as e:
                state.cooldown(e.seconds, padding=5)
                raise Halt(f'{e} Stopped. Cooldown saved; restart later.') from None
        finally:
            state.db.close()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (EOFError, KeyboardInterrupt):
        log('Cancelled. Saved progress was retained.')
        sys.exit(2)
    except Halt as e:
        log(str(e))
        sys.exit(2)
    except Exception as e:
        # Never dump request URLs, tokens or authentication state in tracebacks.
        log(f'Stopped on {type(e).__name__}. Progress retained; use --status to inspect pending sends.')
        sys.exit(1)

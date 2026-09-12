# Telegram channel copier

Configured job: source **-1003571991185**, destination **-1004455533802**, message IDs **4 through 56521 inclusive**. There are **56,518 ID slots**; deleted messages mean fewer actual posts.

This copies existing messages using Telegram's `copyMessages` endpoint. There is no “Forwarded from” tag. Original captions are retained, and each album's available members in the requested range are submitted together. Photos, videos, audio, voice notes, documents, animations, stickers, text and other message types that Telegram permits copying are handled by Telegram itself. It does not download, recompress or upload your media.

## Set up on Ubuntu / a VPS

Requires Python 3.10 or newer. Extract this ZIP, then:

```bash
cd telegram_copier
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
nano .env
```

Fill in all three values:

```dotenv
BOT_TOKEN=YOUR_BOT_TOKEN
API_ID=YOUR_NUMERIC_API_ID
API_HASH=YOUR_API_HASH
```

Get `API_ID` and `API_HASH` from https://my.telegram.org under **API development tools**. They are different from your bot token. The program logs in **as your bot**; it does not require a user-account session or your phone login code. Add the bot as administrator of both channels, with **Post Messages** enabled in the destination. Editing or deleting destination messages is not required.

Why the extra credentials? The HTTP Bot API can copy known IDs, but does not expose a general historical-message lookup to discover album boundaries. Telethon reads the specified IDs using `channels.getMessages`; the normal Bot API copies the resulting complete album units. No separate userbot is used.

Start:

```bash
python copier.py
```

It first scans metadata in batches of 100 IDs and saves each scan checkpoint. Only after scanning the range does it start copying. With the default one-second pause per metadata batch, scanning adds roughly ten minutes plus network time. Scan progress survives restarts.

For a source-only inspection first:

```bash
python copier.py --scan-only
```

The inspection validates destination permissions but does not post there.

## Progress and restarting

Check progress, including while the copier is running:

```bash
python copier.py --status
```

Stop gracefully with **Ctrl+C**, then restart with the same command:

```bash
python copier.py
```

The `state/` folder, beside the script, contains:

| File | Purpose |
|---|---|
| `progress.sqlite3` | Authoritative scan, copy-operation journal, counters and cooldown |
| `msg.txt` | Readable JSON checkpoint, next ID, remaining IDs and pending-send details |
| `bot.session` | Telethon bot login session |
| `copier.lock` | Prevents two copiers using the same state directory |

Keep the entire state directory. Stop the program before taking a filesystem backup; retain any SQLite `-wal` / `-shm` sidecars that are present. The session file and `.env` contain secrets and should not be shared.

`last_processed_id` means the last source ID handled, including missing or skipped entries. For example, after copying 4 and discovering that 5 is absent, it records 5 and then copies 6. It never moves backwards to retry a deleted ID.

`remaining_id_slots` is `56521 - last_processed_id`. After scanning finishes, `remaining_scanned_by_kind.copy` estimates the actual messages still queued. It is a snapshot: later source deletions or changes can affect delivery. `copied_messages` counts confirmed destination messages, not scan attempts or albums. A ten-item album counts as ten messages.

SQLite takes precedence over `msg.txt`. If SQLite is unavailable but a valid `msg.txt` remains, a fresh database resumes after its checkpoint and rescans the remaining range. This loses the old per-operation audit history. An unresolved send recorded in `msg.txt` requires restoring SQLite first. **Do not edit the checkpoint to a larger ID**: that would omit messages. A smaller ID can cause duplicates or split an album.

## Rate limits and speed

Telegram's official bot FAQ says to avoid more than one message per second in a single chat, gives 20 messages per minute for a group, and about 30 messages per second for ordinary bulk broadcasts. The broadcast allowance is not a 30-message-per-second allowance for one destination. Telegram does not publish a universally safe archive-copy speed or a separate guaranteed album quota.

The script's default **3 seconds per message** is a conservative engineering choice, not a guarantee. A ten-item album is copied in one request, then incurs a 30-second pause. Every destination message counts toward pacing. If other programs use the same bot, their traffic also matters.

| Selected pace | Nominal single-message throughput | Pacing time for 56,518 actual messages |
|---|---:|---:|
| 3 seconds, default | 20/minute | 47.1 hours |
| 2 seconds, more aggressive | 30/minute | 31.4 hours |
| 5 seconds, slower | 12/minute | 78.5 hours |

These estimates exclude metadata scanning, HTTP request time, manual reviews and Telegram cooldowns. Missing IDs reduce copying work. Example for a slower run:

```bash
python copier.py --seconds-per-message 5
```

On HTTP **429**, the program saves `retry_after` plus five seconds and **exits**, as requested. During the metadata scan, a Telethon **FLOOD_WAIT** also saves the wait and exits. It will refuse to resume before the saved deadline expires. Afterwards, run `python copier.py` again. Permission errors and other rejections also stop without pretending that the current messages were copied.

This program does not enable paid broadcasts, rotate tokens or try to evade spam restrictions. Slowing it down does not override protected-content or access restrictions.

## Interrupted or partial sends

The Bot API has no client-supplied idempotency key for `copyMessages`. If Telegram accepts a copy but the connection drops before the reply arrives, automatically repeating the request could duplicate the posts. Therefore the operation is written to SQLite before sending and becomes unresolved until delivery is known. A crash during the send behaves the same way.

Run `--status`, inspect its `source_ids`, and check the destination manually. Then use one of these commands:

**All items were copied:** provide their actual destination message IDs in source order:

```bash
python copier.py --resolve done --destination-ids 9001,9002,9003
```

**None were copied:** allow a retry, then run normally:

```bash
python copier.py --resolve retry
python copier.py
```

Do not select retry when some or all posts already arrived. If an uncertain operation delivered only part of an album, restore a clean destination state for that operation before retrying, or investigate it manually. The script does not delete posts for you.

**Telegram explicitly returned a partial success:** it saved the returned destination IDs and stopped. If you accept the omitted items as skipped:

```bash
python copier.py --resolve accept-partial
python copier.py
```

The audit records a partial result without inventing source-to-destination mappings for the omitted entries. There is no promise of automatic exactly-once delivery after an ambiguous network failure.

## What “as it is” can preserve

- Albums are grouped across metadata batch boundaries; holes in IDs do not split them. Album members outside the specified start/end range are deliberately excluded.
- Existing media and captions are copied through Telegram; messages get new destination IDs and posting times. Original views, reactions, comments, reply relationships and interactive bot behavior are not migrated. Treat this as a content copier, not a complete channel backup.
- Content protection is respected. An entirely protected source stops the job; individually protected posts are recorded as skipped. Deleted and service messages are also skipped.
- Telegram excludes certain content from copying, such as paid media, giveaways and invoices; quiz polls have additional requirements. If Telegram omits a candidate during copying, the program saves a partial result and stops for review rather than silently claiming success.
- The manifest is a scan snapshot, not a live synchronization service. Avoid editing or deleting the source during the migration. No messages after ID 56521 are copied.
- This code includes no running bot listener or bot commands; it is a finite archival-copy job authenticated as a bot.

## Verification

Twelve offline tests cover missing IDs, albums crossing scan batches, persisted cooldowns, database/text recovery, uncertain sends, partial results, permission failures, metadata classification and token-safe HTTP errors:

```bash
python -m unittest -v
```

Tested with Python 3.12 and Telethon 1.45.0. No live channel transfer was tested because credentials were not supplied.

## Official references checked 12 September 2026

- Copy behavior, albums, limitations and batch size: https://core.telegram.org/bots/api#copymessages
- Retry-after response parameter: https://core.telegram.org/bots/api#responseparameters
- Published bot rate guidance: https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
- Metadata lookup, usable by bots: https://core.telegram.org/method/channels.getMessages
- Bot access-hash handling: https://core.telegram.org/api/peers#access-hash
- Telethon bot sign-in and API credentials: https://docs.telethon.dev/en/stable/basic/signing-in.html

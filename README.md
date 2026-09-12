# Telegram channel transfer

Copy or download/upload a chosen message range with album-aware progress and restart recovery.

Default source: **-1003571991185**. Default destination: **-1004455533802**. Default range: **4–56521 inclusive** (56,518 ID slots, including holes).

## Update your existing installation

Stop this copier with Ctrl+C first. The unrelated bot process on the other VPS can stay running.

```bash
cd ~/frwd
git pull --ff-only
python3 -m pip install -r requirements.txt
python3 copier.py
```

If you use a virtual environment, activate it before installing dependencies and running the script. Keep your existing `.env` and `state/` directory. SQLite is migrated without resetting progress. The existing failure on a protected source did not advance the checkpoint.

Running `python3 copier.py` in a terminal now asks:

```text
First message link or ID [https://t.me/c/3571991185/4]:
Last message link or ID [https://t.me/c/3571991185/56521]:
```

Paste the first and last links, or enter numeric IDs. Press Enter to accept the shown defaults. Both links must belong to the same source channel. The end is inclusive. Private-channel links and public-channel message links are supported; forum-topic links are not.

If the source is protected, it then asks:

```text
Source has content protection enabled.
Download and upload this range instead? [Y/n]:
```

Choose **Y**. The mode is saved for that job. This uses normal authenticated media downloads and fresh uploads; it does not change the source channel's settings. If Telegram refuses the bot's download, the transfer stops and retains progress. No transfer can guarantee that every protected source is downloadable.

## First installation

Python 3.10+ is required. Clone this repository or use GitHub's **Code → Download ZIP**. The bundled `telegram_copier.zip` is also updated with the source release.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
nano .env
```

Set:

```dotenv
BOT_TOKEN=YOUR_BOT_TOKEN
API_ID=YOUR_NUMERIC_API_ID
API_HASH=YOUR_API_HASH
```

Obtain `API_ID` and `API_HASH` at https://my.telegram.org under **API development tools**. These are application credentials. The script logs in **as the bot**, without a user-account session or phone-code prompt. Add the bot as administrator of both channels; the destination requires **Post Messages** permission.

Do not share `.env`, the database, or another process's Telegram session. No bot token is embedded in the repository.

## Commands

Interactive range selection and automatic mode prompt:

```bash
python3 copier.py
```

Resume the last selected job without range prompts:

```bash
python3 copier.py --resume
```

Explicit download/upload job, suitable for a noninteractive terminal:

```bash
python3 copier.py \
  --from https://t.me/c/3571991185/4 \
  --to https://t.me/c/3571991185/56521 \
  --destination -1004455533802 \
  --mode upload
```

Progress, including while the writer is running:

```bash
python3 copier.py --status
```

Other options:

| Option | Meaning |
|---|---|
| `--mode ask` | Default. Use copying normally; offer upload when protection is found. Remember a previously selected upload mode. |
| `--mode copy` | Require server-side copying; stop if protection is detected. |
| `--mode upload` | Download/upload from the beginning, even for an unprotected source. |
| `--scan-only` | Inspect message IDs and albums without posting destination messages. |
| `--seconds-per-message 5` | Slow final posting down to five seconds per message. |
| `--state-dir /absolute/path` | Use one explicit job directory, bypassing automatic job selection. |

`--resume`, `--status` and `--resolve` use a saved range. To select another range, use the regular prompts or `--from` and `--to`. A distinct range gets a distinct job directory; overlapping ranges can intentionally copy the same posts again.

## Media and album preservation

The script scans the specified IDs before posting, saving each 100-ID scan checkpoint. It groups album members across scan-batch boundaries. If 5 is absent, it processes 4, records the missing 5, and proceeds to 6.

In upload mode it:

1. Fetches the current source messages for one album or standalone post.
2. Downloads the media to a temporary directory.
3. Carries over original caption text and formatting entities, per-item spoiler flags, document MIME type and attributes (including video dimensions/duration/streaming flags, audio information and original filename).
4. Downloads the largest available regular source thumbnail and an explicit video cover, when present. A valid source thumbnail JPEG is kept unchanged; oversized or non-JPEG thumbnails are normalized for Telegram. If no regular thumbnail exists, it logs that limitation and Telegram may generate a preview. Telegram ultimately controls thumbnail rendering.
5. Registers uploaded media objects, then sends the album in **one SendMultiMedia request**. This registration step creates no destination posts. Standalone items remain standalone.
6. Commits confirmed destination IDs, then removes temporary files.

Photo/video albums and file/audio albums are kept together. More than ten upload-album items, an interleaved source album, changed album membership or inconsistent caption placement causes a stop rather than silently splitting or reordering the posts. Only album members within the selected range are included; choose full-album boundaries when entering your links.

Text, photos, videos, documents, audio, voice/video notes and common sticker/animation files are supported. Contacts and static locations/venues are reconstructed. Special messages that cannot be faithfully reuploaded (for example dice, poll results, paid media, expiring media and live photos) stop the upload fallback for review. Copy mode supports whatever Telegram's `copyMessages` permits.

“As it is” means preserving transferable content and layout, not a complete channel backup: messages receive new IDs and dates. Views, reactions, comments, reply relationships and interactive bot buttons are not migrated. Web previews can be regenerated. Telegram may process uploaded photos, thumbnails or other metadata; byte-identical visual presentation is not guaranteed. No local video recompression is performed.

## Coexistence with a bot on another VPS

This is a finite transfer program, not a Telegram update listener:

- It does **not** call Bot API `getUpdates`, `setWebhook` or `deleteWebhook`.
- It uses a fresh in-memory MTProto session for each invocation; it never reuses or modifies another process's `.session` file, nor the old `state/bot.session`.
- Update delivery, catch-up, login-time difference fetching and the update dispatcher are disabled. The two internal hooks are tested against the pinned **Telethon 1.45.0**; do not casually remove the version pin.
- A local kernel lock prevents two writers from using the same job database. Status is read-only and remains available while that job runs.
- Upload files use a job-specific temporary directory and are cleaned up on ordinary completion/errors. A hard kill can leave a `transfers/unit-*` folder in that job; after stopping its copier, that leftover folder may be deleted.

This avoids the usual polling/webhook and shared-session-key conflicts. **Telegram limits still apply across all uses of the same bot token.** A process on another VPS can cause this job to be rate-limited. Separate VPS jobs are not coordinated by the local lock: if both write to the same destination, posts can interleave or duplicate. Use a separate bot token for independent quotas, or coordinate the destination/job with the other operator. No program can promise zero Telegram or network errors.

## Progress, recovery and limits

Legacy installations keep using the matching `state/progress.sqlite3`. New ranges live under `state/jobs/<source>_<destination>_<start>_<end>/`. `state/active_job.json` remembers the current selection. Each job contains its authoritative `progress.sqlite3`, readable JSON `msg.txt`, and `copier.lock`.

`last_processed_id` includes copied IDs and missing/service entries. `remaining_id_slots` is the end ID minus that checkpoint; after scanning, `remaining_scanned_by_kind.copy` counts queued message candidates. `copied_messages` counts confirmed destination posts: a ten-item album counts as ten. The scan is a snapshot; source edits/deletions during migration can change the final count.

SQLite takes precedence over `msg.txt`. A valid text checkpoint can initialize a replacement database if the database is lost and no send is unresolved. Remaining IDs are rescanned; the old detailed audit trail is not recreated. Never manually advance the checkpoint to hide a failure. Preserve the whole state directory; stop the program before filesystem backups and keep any SQLite sidecars present.

The default pace is **three seconds per final destination message**, including each album item. A ten-item album incurs a 30-second pause after posting. If all 56,518 IDs exist, pacing alone is about 47.1 hours. Download/upload adds time and bandwidth: approximately each file's size downloaded and uploaded, plus thumbnails. Temporary free disk must accommodate the next album. Telegram's current per-file upload restrictions still apply; large files are uploaded using MTProto rather than the cloud Bot API's multipart-upload path.

Telegram advises avoiding more than one message per second in one chat; its FAQ also lists 20/minute for groups and about 30/second for ordinary broadcasts. These are not a guarantee for this job. Other bot processes contribute traffic. **HTTP 429 or FLOOD_WAIT saves the cooldown plus five seconds and exits**. Run `--resume` after the saved deadline. The job does not automatically evade or retry through a flood restriction.

## Uncertain sends

The database journals each final send before issuing it. Upload operations also store their MTProto random IDs. If sending succeeds but the reply is lost, automatic retries could duplicate posts, so an unresolved operation blocks restart until reviewed. Download/staging failures occur before this journal point and can be retried normally. No automatic exactly-once guarantee is made for ambiguous sends.

Inspect `--status` and the destination. If every item arrived, supply the actual destination IDs in source order:

```bash
python3 copier.py --resolve done --destination-ids 9001,9002,9003
python3 copier.py --resume
```

If none arrived:

```bash
python3 copier.py --resolve retry
python3 copier.py --resume
```

Do not authorize retry if some posts already arrived. Investigate or manually restore a clean destination for that operation first. The script never deletes your posts automatically.

Only when the **Bot API explicitly returned a partial copy result**, you may accept omitted items as skipped:

```bash
python3 copier.py --resolve accept-partial
python3 copier.py --resume
```

## Testing

```bash
python3 -m unittest -v
```

The offline tests cover existing checkpoint recovery, range prompts, protected-mode selection, album staging, original thumbnail selection/normalization, media attributes, ambiguous sends, flood handling, disk-session isolation and suppression of update synchronization. No live transfer is claimed without credentials and a source/destination test.

References:

- https://core.telegram.org/bots/api#copymessages
- https://core.telegram.org/method/channels.getMessages
- https://core.telegram.org/method/messages.sendMultiMedia
- https://core.telegram.org/method/messages.uploadMedia
- https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
- https://docs.telethon.dev/en/stable/modules/client.html

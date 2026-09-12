"""Offline range, migration, upload and coexistence checks. Never contacts Telegram."""
import argparse
import asyncio
import datetime as dt
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from PIL import Image
from telethon import functions, types
from telethon.errors import FloodWaitError

import copier as c
import media_upload as u


def message(mid=4, group=None, media=None, text='Caption', **kwargs):
    return types.Message(id=mid, peer_id=types.PeerChannel(3571991185), message=text,
                         media=media, grouped_id=group, entities=[types.MessageEntityBold(0, len(text))], **kwargs)


def photo(mid=4, group=None):
    obj = types.Photo(id=mid, access_hash=1, file_reference=b'ref', date=dt.datetime.now(dt.timezone.utc),
                      sizes=[types.PhotoSize('x', 300, 300, 100)], dc_id=1)
    return message(mid, group, types.MessageMediaPhoto(photo=obj, spoiler=True))


def video(mid=5, group=None):
    attrs = [types.DocumentAttributeFilename('original-name.mp4'),
             types.DocumentAttributeVideo(duration=12.5, w=640, h=480, supports_streaming=True)]
    obj = types.Document(id=mid, access_hash=1, file_reference=b'ref', date=dt.datetime.now(dt.timezone.utc),
                         mime_type='video/mp4', size=4, dc_id=1, attributes=attrs,
                         thumbs=[types.PhotoSize('m', 320, 180, 50), types.PhotoSize('s', 100, 80, 5)])
    return message(mid, group, types.MessageMediaDocument(document=obj, spoiler=True))


class FakeClient:
    def __init__(self, messages=None, fail=None):
        self.messages = messages or []
        self.fail = fail
        self.calls, self.uploaded, self.downloads = [], [], []

    async def download_media(self, source, file, thumb=None, progress_callback=None):
        self.downloads.append((source.id, thumb))
        if thumb:
            Image.new('RGB', (320, 180), 'navy').save(file, 'JPEG')
        else:
            Path(file).write_bytes(b'data')
        return file

    async def upload_file(self, path, progress_callback=None):
        self.uploaded.append(Path(path).read_bytes())
        return types.InputFile(len(self.uploaded), 1, Path(path).name, 'checksum')

    async def __call__(self, request):
        self.calls.append(request)
        if isinstance(request, functions.channels.GetMessagesRequest):
            return SimpleNamespace(messages=self.messages)
        if isinstance(request, functions.messages.UploadMediaRequest):
            if isinstance(request.media, types.InputMediaUploadedPhoto):
                return photo().media
            return video().media
        if self.fail:
            raise self.fail
        if isinstance(request, functions.messages.SendMultiMediaRequest):
            random_ids = [item.random_id for item in request.multi_media]
        else:
            random_ids = [request.random_id]
        return SimpleNamespace(updates=[types.UpdateMessageID(1000 + i, r) for i, r in enumerate(random_ids)])


class UploadTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)
        self.state = c.State(self.path)
        self.peer = types.InputPeerChannel(4455533802, 0)
        self.source = types.InputChannel(3571991185, 0)
        self.stop = c.STOP
        c.STOP = False

    def tearDown(self):
        c.STOP = self.stop
        self.state.db.close()
        self.temp.cleanup()

    async def test_stage_preserves_video_attributes_and_correct_thumbnail(self):
        client = FakeClient()
        m = video()
        staged = await u.Stager(client, self.peer, self.path, lambda _: None).stage(m)
        uploaded = next(r.media for r in client.calls if isinstance(r, functions.messages.UploadMediaRequest))
        self.assertIsInstance(uploaded, types.InputMediaUploadedDocument)
        self.assertEqual(uploaded.mime_type, 'video/mp4')
        self.assertEqual(uploaded.attributes[0].file_name, 'original-name.mp4')
        self.assertEqual(uploaded.attributes[1].duration, 12.5)
        self.assertTrue(uploaded.attributes[1].supports_streaming)
        self.assertIsNotNone(uploaded.thumb)
        self.assertIn((5, 'm'), client.downloads)  # Source thumbnail index != Telethon's sorted index.
        self.assertTrue(staged.spoiler)
        self.assertFalse(any(isinstance(r, functions.messages.SendMediaRequest) for r in client.calls))

    async def test_mixed_album_is_one_send_with_captions_and_entities(self):
        messages = [photo(4, 999), video(5, 999)]
        client = FakeClient(messages)
        self.state.record_scan([(4, '999', 'copy'), (5, '999', 'copy')], 5)
        await c.send_upload_unit(self.state, client, self.source, self.peer, [4, 5], 5, 3)
        sends = [r for r in client.calls if isinstance(r, functions.messages.SendMultiMediaRequest)]
        self.assertEqual(len(sends), 1)
        self.assertEqual(len(sends[0].multi_media), 2)
        self.assertEqual(sends[0].multi_media[0].message, 'Caption')
        self.assertEqual(sends[0].multi_media[1].entities[0].length, 7)
        self.assertIsInstance(sends[0].multi_media[0].media, types.InputMediaPhoto)
        self.assertIsInstance(sends[0].multi_media[1].media, types.InputMediaDocument)
        self.assertEqual(self.state.get('copied'), 2)
        op = self.state.db.execute('SELECT * FROM operations').fetchone()
        self.assertEqual(op['transport'], 'upload')
        self.assertEqual(len(json.loads(op['random_ids'])), 2)
        self.assertEqual(list((self.path / 'transfers').iterdir()), [])

    async def test_download_failure_has_no_pending_send_or_checkpoint_advance(self):
        client = FakeClient([video(4)])
        async def fail(*args, **kwargs):
            raise OSError('download failed')
        client.download_media = fail
        with self.assertRaises(OSError):
            await c.send_upload_unit(self.state, client, self.source, self.peer, [4], 4, 3)
        self.assertEqual(self.state.get('last'), 3)
        self.assertIsNone(self.state.unresolved())
        self.assertEqual(list((self.path / 'transfers').iterdir()), [])

    async def test_ambiguous_send_survives_restart_as_unresolved(self):
        client = FakeClient([video(4)], fail=OSError('disconnected after send'))
        with self.assertRaises(c.Halt):
            await c.send_upload_unit(self.state, client, self.source, self.peer, [4], 4, 3)
        self.state.db.close()
        self.state = c.State(self.path)
        self.assertEqual(self.state.unresolved()['state'], 'uncertain')
        self.assertEqual(self.state.get('last'), 3)

    async def test_upload_flood_stops_with_persisted_cooldown(self):
        client = FakeClient([video(4)], fail=FloodWaitError(request=None, capture=42))
        with self.assertRaises(c.Limited):
            await c.send_upload_unit(self.state, client, self.source, self.peer, [4], 4, 3)
        self.assertGreater(self.state.get('not_before'), c.time.time() + 45)
        self.assertEqual(self.state.get('last'), 3)
        self.assertIsNone(self.state.unresolved())

    async def test_stop_during_staging_never_posts(self):
        client = FakeClient([video(4)])
        c.STOP = True
        with self.assertRaises(u.UploadStopped):
            await c.send_upload_unit(self.state, client, self.source, self.peer, [4], 4, 3)
        self.assertIsNone(self.state.unresolved())
        self.assertFalse(any(isinstance(r, functions.messages.SendMediaRequest) for r in client.calls))

    async def test_no_thumbnail_logs_limitation_instead_of_failing(self):
        m = video()
        m.media.document.thumbs = []
        notices = []
        client = FakeClient()
        await u.Stager(client, self.peer, self.path, notices.append).stage(m)
        self.assertTrue(any('no downloadable original thumbnail' in n for n in notices))

    async def test_changed_album_stops_before_upload(self):
        client = FakeClient([photo(4, 10), video(5, 11)])
        with self.assertRaises(u.UploadProblem):
            await c.send_upload_unit(self.state, client, self.source, self.peer, [4, 5], 5, 3)
        self.assertEqual(client.uploaded, [])
        self.assertEqual(self.state.get('last'), 3)

    async def test_missing_after_scan_replans_without_posting(self):
        self.state.record_scan([(4, None, 'copy')], 4)
        await c.send_upload_unit(self.state, FakeClient([]), self.source, self.peer, [4], 4, 3)
        row = self.state.db.execute('SELECT kind FROM messages WHERE id=4').fetchone()
        self.assertEqual(row[0], 'missing')
        self.assertIsNone(self.state.unresolved())

    async def test_fresh_client_has_no_updates_and_no_disk_session(self):
        from telethon.sessions import MemorySession
        client = c.create_client(12345, 'hash')
        self.assertIsInstance(client.session, MemorySession)
        self.assertTrue(client._no_updates)
        self.assertFalse(client._catch_up)
        # A login must not issue Telethon's default GetState/GetDifference calls.
        await client._on_login(SimpleNamespace(id=99, bot=True, access_hash=123))
        self.assertEqual(client._mb_entity_cache.self_id, 99)
        await client._update_loop()
        await client.disconnect()


class RangeTests(unittest.TestCase):
    def setUp(self):
        self.original = c.SOURCE, c.DESTINATION, c.START, c.END, c.JOB, c.TRANSFER_MODE

    def tearDown(self):
        c.SOURCE, c.DESTINATION, c.START, c.END, c.JOB, c.TRANSFER_MODE = self.original

    def args(self, directory, **changes):
        values = dict(state_dir=str(directory), status=False, resolve=None, resume=False,
                      from_message='4', to_message='12', destination=None)
        values.update(changes)
        return argparse.Namespace(**values)

    def test_private_and_numeric_links(self):
        self.assertEqual(c.parse_reference('https://t.me/c/3571991185/4'), (-1003571991185, 4))
        self.assertEqual(c.parse_reference('56521'), (None, 56521))
        with self.assertRaises(c.Halt): c.parse_reference('https://evil.test/c/3571991185/4')
        with self.assertRaises(c.Halt): c.parse_reference('https://t.me/c/3571991185/1/4')
        with self.assertRaises(c.Halt): c.parse_reference('0')

    def test_mismatched_channels_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            args = self.args(temp, from_message='https://t.me/c/3571991185/4', to_message='https://t.me/c/4455533802/12')
            with self.assertRaises(c.Halt): c.configure_job(args, None)

    def test_interactive_prompts_precede_any_network(self):
        with tempfile.TemporaryDirectory() as temp:
            args = self.args(temp, from_message=None, to_message=None)
            with patch('sys.stdin.isatty', return_value=True), patch('builtins.input', side_effect=['https://t.me/c/3571991185/10','20']) as prompt:
                c.configure_job(args, None)
            self.assertEqual(prompt.call_count, 2)
            self.assertEqual(c.START, 10)
            self.assertEqual(c.END, 20)

    def test_legacy_checkpoint_is_retained_on_resume(self):
        with tempfile.TemporaryDirectory() as temp:
            state = c.State(temp)
            with state.db:
                state.set('last', 50)
                state.set('scan', 100)
            state.mirror()
            state.db.close()
            args = self.args(temp, resume=True, from_message=None, to_message=None)
            directory = c.configure_job(args, None)
            state = c.State(directory)
            self.assertEqual(state.get('last'), 50)
            self.assertEqual(state.get('scan'), 100)
            state.db.close()

    def test_protected_mode_converts_pending_without_resetting_progress(self):
        with tempfile.TemporaryDirectory() as temp:
            state = c.State(temp)
            state.record_scan([(4, None, 'protected'), (5, None, 'copy')], 5)
            c.enable_upload(state, 'upload', 'Protected')
            self.assertEqual(c.next_unit(state), ([4], 4))
            self.assertEqual(state.get('last'), 3)
            self.assertEqual(state.get('mode'), 'upload')
            state.db.close()

    def test_gap_does_not_silently_skip_next_protected_message(self):
        with tempfile.TemporaryDirectory() as temp:
            state = c.State(temp)
            state.record_scan([(4, None, 'missing'), (5, None, 'protected'), (6, None, 'copy')], 6)
            self.assertEqual(c.next_unit(state), ([], 4))
            state.db.close()

    def test_thumbnail_normalization_preserves_valid_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'thumb.jpg'
            Image.new('RGB', (300, 150), 'green').save(path, 'JPEG')
            data = path.read_bytes()
            self.assertEqual(u.normalize_thumbnail(path).read_bytes(), data)
            large = Path(temp) / 'big.png'
            Image.new('RGB', (1000, 700), 'green').save(large)
            normalized = u.normalize_thumbnail(large)
            with Image.open(normalized) as im:
                self.assertLessEqual(max(im.size), 320)
                self.assertEqual(im.format, 'JPEG')

    def test_incomplete_response_never_assumes_zero_delivery(self):
        with self.assertRaises(u.UploadProblem):
            u.extract_ids(SimpleNamespace(updates=[]), [101, 102])

    def test_standalone_text_has_entities_and_no_forward_header(self):
        request = u.build_request(types.InputPeerChannel(1, 0), [message()], [None], [123])
        self.assertIsInstance(request, functions.messages.SendMessageRequest)
        self.assertEqual(request.entities[0].length, 7)
        self.assertEqual(request.random_id, 123)

    def test_unsupported_dice_does_not_silently_reroll(self):
        with tempfile.TemporaryDirectory() as temp:
            stager = u.Stager(FakeClient(), types.InputPeerChannel(1, 0), temp, lambda _: None)
            with self.assertRaises(u.UploadProblem):
                asyncio.run(stager.stage(message(media=types.MessageMediaDice(6, '🎲'))))


if __name__ == '__main__':
    unittest.main()

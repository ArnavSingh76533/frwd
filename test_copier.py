"""Offline tests. No requests to Telegram and no real bot token needed."""
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import copier as c


class FakeAPI:
    def __init__(self, result=None, error=None):
        self.result, self.error, self.calls = result, error, []

    def call(self, method, payload):
        self.calls.append((method, payload))
        if self.error:
            raise self.error
        return self.result


class CopierTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.state = c.State(self.directory)

    def tearDown(self):
        self.state.db.close()
        self.temp.cleanup()

    def reopen(self):
        self.state.db.close()
        self.state = c.State(self.directory)

    def seed(self, rows):
        self.state.record_scan(rows, max(r[0] for r in rows))

    def test_missing_5_advances_to_6_after_copy_4(self):
        self.seed([(4, None, 'copy'), (5, None, 'missing'), (6, None, 'copy')])
        api = FakeAPI([{'message_id': 100}])
        c.send_unit(self.state, api, *c.next_unit(self.state), 3)
        ids, through = c.next_unit(self.state)
        self.assertEqual((ids, through), ([], 5))
        self.state.skip(through)
        self.assertEqual(c.next_unit(self.state), ([6], 6))
        self.assertEqual(self.state.get('copied'), 1)
        self.assertEqual(self.state.get('skipped'), 1)

    def test_album_across_scan_boundary_with_hole(self):
        self.seed([(i, None, 'missing') for i in range(4, 102)] + [(102, 'album', 'copy'), (103, 'album', 'copy')])
        self.seed([(104, None, 'missing'), (105, 'album', 'copy'), (106, None, 'copy')])
        self.state.skip(101)
        ids, through = c.next_unit(self.state)
        self.assertEqual(ids, [102, 103, 105])
        self.assertEqual(through, 105)
        api = FakeAPI([{'message_id': i} for i in (400, 401, 402)])
        before = time.time()
        c.send_unit(self.state, api, ids, through, 3)
        self.assertGreaterEqual(self.state.get('not_before'), before + 9)
        self.assertLess(self.state.get('not_before'), time.time() + 10)
        self.assertEqual(api.calls[0][0], 'copyMessages')
        self.assertFalse(api.calls[0][1]['remove_caption'])
        self.assertEqual(c.next_unit(self.state), ([106], 106))

    def test_restart_uses_sqlite_even_if_text_is_stale(self):
        self.seed([(4, None, 'copy'), (5, None, 'copy')])
        stale = self.state.txt.read_text()
        c.send_unit(self.state, FakeAPI([{'message_id': 90}]), [4], 4, 3)
        self.state.txt.write_text(stale)
        self.reopen()
        self.assertEqual(self.state.get('last'), 4)
        self.assertEqual(json.loads(self.state.txt.read_text())['last_processed_id'], 4)
        self.assertEqual(c.next_unit(self.state), ([5], 5))

    def test_flood_does_not_advance(self):
        self.seed([(4, None, 'copy')])
        with self.assertRaises(c.Limited):
            c.send_unit(self.state, FakeAPI(error=c.Limited(40)), [4], 4, 3)
        self.assertEqual(self.state.get('last'), 3)
        self.assertIsNone(self.state.unresolved())
        self.state.cooldown(40, padding=5)
        self.reopen()
        self.assertGreater(self.state.get('not_before'), time.time() + 43)

    def test_network_failure_retains_pending_across_restart(self):
        self.seed([(4, None, 'copy')])
        with self.assertRaises(c.Halt):
            c.send_unit(self.state, FakeAPI(error=c.Halt('network failure')), [4], 4, 3)
        self.reopen()
        self.assertEqual(self.state.unresolved()['state'], 'uncertain')
        self.assertEqual(self.state.get('last'), 3)
        self.assertTrue(json.loads(self.state.txt.read_text())['unresolved_operation'])
        c.resolve(self.state, 'done', '888')
        self.assertEqual(self.state.get('last'), 4)
        self.assertEqual(self.state.get('copied'), 1)

    def test_crash_after_prepare_stays_unresolved(self):
        self.seed([(4, None, 'copy')])
        self.state.prepare([4], 4)
        self.reopen()
        self.assertEqual(self.state.unresolved()['state'], 'sending')
        self.assertEqual(self.state.get('last'), 3)

    def test_permission_failure_is_not_skipped(self):
        self.seed([(4, None, 'copy')])
        with self.assertRaises(c.Rejected):
            c.send_unit(self.state, FakeAPI(error=c.Rejected(403, 'Forbidden')), [4], 4, 3)
        self.assertEqual(self.state.get('last'), 3)
        self.assertEqual(self.state.get('skipped'), 0)

    def test_partial_requires_explicit_acceptance(self):
        self.seed([(4, 'g', 'copy'), (5, 'g', 'copy')])
        with self.assertRaises(c.Halt):
            c.send_unit(self.state, FakeAPI([{'message_id': 90}]), [4, 5], 5, 3)
        self.assertEqual(self.state.get('last'), 3)
        self.reopen()
        self.assertEqual(self.state.unresolved()['state'], 'partial')
        c.resolve(self.state, 'accept-partial', '')
        self.assertEqual(self.state.get('copied'), 1)
        self.assertEqual(self.state.get('skipped'), 1)
        self.assertEqual(self.state.get('last'), 5)

    def test_text_checkpoint_can_bootstrap_a_new_db(self):
        self.seed([(4, None, 'copy')])
        c.send_unit(self.state, FakeAPI([{'message_id': 9}]), [4], 4, 3)
        with tempfile.TemporaryDirectory() as other:
            Path(other, 'msg.txt').write_text(self.state.txt.read_text())
            restored = c.State(other)
            self.assertEqual(restored.get('last'), 4)
            self.assertEqual(restored.get('scan'), 4)
            self.assertEqual(restored.get('copied'), 1)
            restored.db.close()

    def test_interleaved_album_stops_instead_of_reordering(self):
        self.seed([(4, 'g', 'copy'), (5, None, 'copy'), (6, 'g', 'copy')])
        with self.assertRaises(c.Halt):
            c.next_unit(self.state)

    def test_classification(self):
        from telethon import types
        self.assertEqual(c.classify(types.MessageEmpty(id=5, peer_id=types.PeerChannel(1))), 'missing')
        self.assertEqual(c.classify(None), 'missing')
        self.assertEqual(c.classify(types.MessageService(id=6, peer_id=types.PeerChannel(1), date=None, action=types.MessageActionEmpty())), 'service')
        self.assertEqual(c.classify(types.Message(id=7, peer_id=types.PeerChannel(1), message='hello', noforwards=True)), 'protected')
        self.assertEqual(c.classify(types.Message(id=8, peer_id=types.PeerChannel(1), message='hello')), 'copy')

    def test_http_429_and_token_not_in_network_errors(self):
        api = c.BotAPI('12345:not-a-real-token')
        response = type('Response', (), {'status_code': 429, 'json': lambda _: {'ok': False, 'error_code': 429, 'parameters': {'retry_after': 123}}})()
        with patch.object(api.session, 'post', return_value=response):
            with self.assertRaises(c.Limited) as caught:
                api.call('copyMessages', {})
            self.assertEqual(caught.exception.seconds, 123)
        with patch.object(api.session, 'post', side_effect=api.requests.Timeout(api.base)):
            with self.assertRaises(c.Halt) as caught:
                api.call('copyMessages', {})
            self.assertNotIn('not-a-real-token', str(caught.exception))


if __name__ == '__main__':
    unittest.main()

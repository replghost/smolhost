#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Offline harness-logic tests; these are NOT live content/timeout evidence.

python3 -m unittest discover -s scripts -p 'test_content_soak.py'
"""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("content_soak", Path(__file__).with_name("content-soak.py"))
content = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = content
SPEC.loader.exec_module(content)


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "download.bin"
        self.data = b"independently pinned bytes"
        self.fixture = content.Fixture("bafyfixture", hashlib.sha256(self.data).hexdigest(), len(self.data), 3)
        self.path.write_bytes(self.data)

    def test_verified_receipt_and_bytes_delete_only_own_completed_file(self):
        sibling = self.path.with_name("keep")
        sibling.write_text("unrelated")
        receipt = content.verify_download(self.path, json.dumps(self.fixture.receipt()), self.fixture)
        self.assertEqual(receipt, self.fixture.receipt())
        self.assertFalse(self.path.exists())
        self.assertEqual(sibling.read_text(), "unrelated")

    def test_receipt_cannot_substitute_for_hashing_real_bytes(self):
        self.path.write_bytes(b"x" * len(self.data))
        with self.assertRaisesRegex(content.Failure, "SHA256"):
            content.verify_download(self.path, json.dumps(self.fixture.receipt()), self.fixture)
        self.assertTrue(self.path.exists())

    def test_truncated_file_survives_failed_verification(self):
        self.path.write_bytes(self.data[:-1])
        with self.assertRaisesRegex(content.Failure, "size"):
            content.verify_download(self.path, json.dumps(self.fixture.receipt()), self.fixture)
        self.assertTrue(self.path.exists())

    def test_false_missing_extra_or_multiple_receipts_fail_without_deletion(self):
        receipt = self.fixture.receipt()
        bad = ["", "null", "[]", json.dumps(receipt) + "\n" + json.dumps(receipt),
               json.dumps({**receipt, "blocks": 2}), json.dumps({**receipt, "cid": "different"}),
               json.dumps({**receipt, "event": "completed"})]
        for stdout in bad:
            with self.subTest(stdout=stdout), self.assertRaises(content.Failure):
                content.verify_download(self.path, stdout, self.fixture)
            self.assertTrue(self.path.exists())

    def test_boolean_does_not_pass_integer_receipt_pin(self):
        fixture = content.Fixture(self.fixture.cid, self.fixture.sha256, self.fixture.bytes, 1)
        with self.assertRaises(content.Failure):
            content.verify_download(self.path, json.dumps({**fixture.receipt(), "blocks": True}), fixture)
        self.assertTrue(self.path.exists())

    def test_symlink_never_followed_or_removed(self):
        target = self.path.with_name("target")
        self.path.rename(target)
        self.path.symlink_to(target)
        with self.assertRaises(OSError):
            content.verify_download(self.path, json.dumps(self.fixture.receipt()), self.fixture)
        self.assertTrue(self.path.is_symlink())
        self.assertEqual(target.read_bytes(), self.data)


class InputSocket:
    """A deterministic byte source, not a network or backpressure simulation."""
    def __init__(self, data, fragment=3):
        self.data = bytearray(data)
        self.fragment = fragment
        self.requests = []

    def recv(self, size):
        self.requests.append(size)
        size = min(size, self.fragment)
        result = bytes(self.data[:size])
        del self.data[:size]
        return result


def frame(opcode, data):
    if len(data) < 126:
        return bytes((0x80 | opcode, len(data))) + data
    return bytes((0x80 | opcode, 126)) + struct.pack("!H", len(data)) + data


def reader(data):
    peer = content.WebSocket.__new__(content.WebSocket)
    peer.socket = InputSocket(data)
    peer.pump = Mock()
    peer.deadline = float("inf")
    return peer


class WireReaderTests(unittest.TestCase):
    def test_coalesced_frames_do_not_read_ahead_into_nonreader_payload(self):
        second = frame(2, b"next partial data" * 100)
        peer = reader(frame(2, b"first partial") + second)
        self.assertEqual(peer.frame(), (2, b"first partial"))
        self.assertEqual(bytes(peer.socket.data), second)

    def test_extended_frame_size_is_read_exactly(self):
        payload = bytes(range(256)) * 4
        peer = reader(frame(2, payload))
        self.assertEqual(peer.frame(), (2, payload))

    def test_eof_is_failure_not_expected_adversarial_success(self):
        for raw in (b"", b"\x82", b"\x82\x08short"):
            with self.subTest(raw=raw), self.assertRaisesRegex(content.Failure, "disconnected"):
                reader(raw).frame()

    def test_masked_fragmented_and_oversized_frames_are_failures(self):
        for raw in (b"\x02\x00", b"\x82\x80", b"\xc2\x00",
                    b"\x82\x7f" + struct.pack("!Q", 65537)):
            with self.subTest(raw=raw), self.assertRaises(content.Failure):
                reader(raw).frame()

    def test_read_deadline_does_not_count_empty_or_partial_as_success(self):
        peer = reader(frame(2, b"data"))
        peer.deadline = 5
        with patch.object(content.time, "monotonic", return_value=5):
            with self.assertRaisesRegex(content.Failure, "deadline"):
                peer.frame()
        self.assertEqual(peer.socket.requests, [])

    def test_started_is_not_partial_byte_evidence(self):
        fixture = content.Fixture("bcid", "0" * 64, 100, 2)
        started = json.dumps({"event": "started", "version": 1, "cid": fixture.cid,
                              "chain": content.CHAIN}).encode()
        for next_frame in (frame(2, b""), frame(1, b'{"event":"completed"}'), frame(2, b"x" * 100)):
            peer = reader(frame(1, started) + next_frame)
            peer.send = Mock()
            with self.subTest(next_frame=next_frame), self.assertRaises(content.Failure):
                peer.partial(fixture)


class AdversaryEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.harness = content.ContentSoak(SimpleNamespace())
        self.addCleanup(self.harness.selector.close)
        self.harness.socket = "/unused/offline"
        self.harness.end = 200
        self.harness.record = Mock()
        self.fixture = content.Fixture("bcid", "0" * 64, 10000, 10)
        self.peer = Mock()
        self.peer.partial.return_value = 100
        self.clock = 0

    def pump(self, timeout=0.1):
        self.clock += 1

    def test_slow_case_needs_specific_daemon_timeout_not_elapsed_time(self):
        self.harness.pump = self.pump
        with patch.object(content, "WebSocket", return_value=self.peer), \
                patch.object(content.time, "monotonic", side_effect=lambda: self.clock):
            with self.assertRaisesRegex(content.Failure, "did not produce"):
                self.harness.adversary(self.fixture, slow=True)
        self.assertEqual(self.harness.counts["writer_timeouts_observed"], 0)
        self.peer.close.assert_called_once()
        self.peer.frame.assert_not_called()
        self.assertIsNone(self.harness.active_peer)

    def test_delayed_log_after_three_write_deadlines_is_accepted_without_reading(self):
        def pump(timeout=0.1):
            self.pump(timeout)
            if self.clock == 15:
                self.harness.daemon_timeouts += 1
        self.harness.pump = pump
        with patch.object(content, "WebSocket", return_value=self.peer), \
                patch.object(content.time, "monotonic", side_effect=lambda: self.clock):
            self.harness.adversary(self.fixture, slow=True)
        self.assertEqual(self.harness.counts["writer_timeouts_observed"], 1)
        self.peer.frame.assert_not_called()
        self.peer.close.assert_called_once()

    def test_premature_timeout_evidence_is_not_accepted(self):
        def pump(timeout=0.1):
            self.pump(timeout)
            self.harness.daemon_timeouts += 1
        self.harness.pump = pump
        with patch.object(content, "WebSocket", return_value=self.peer), \
                patch.object(content.time, "monotonic", side_effect=lambda: self.clock):
            with self.assertRaisesRegex(content.Failure, "premature"):
                self.harness.adversary(self.fixture, slow=True)
        self.assertEqual(self.harness.counts["writer_timeouts_observed"], 0)

    def test_completed_stream_cannot_pass_as_cancelled_partial(self):
        self.peer.frame.return_value = (2, b"x" * 9900)
        with patch.object(content, "WebSocket", return_value=self.peer), \
                patch.object(content.time, "monotonic", return_value=0):
            with self.assertRaisesRegex(content.Failure, "complete file"):
                self.harness.adversary(self.fixture, slow=False)
        self.assertEqual(self.harness.counts["cancelled_partial"], 0)
        self.peer.close.assert_called_once()

    def test_cancellation_requires_partial_then_peer_close(self):
        self.peer.frame.side_effect = [(2, b"queued bytes"), (8, b"")]
        with patch.object(content, "WebSocket", return_value=self.peer), \
                patch.object(content.time, "monotonic", return_value=0):
            self.harness.adversary(self.fixture, slow=False)
        self.peer.send.assert_called_once_with({"event": "cancel"})
        self.assertEqual(self.harness.counts["cancelled_partial"], 1)
        self.peer.close.assert_called_once()


class OwnedProcess:
    def __init__(self, stubborn=False):
        self.code = None
        self.stubborn = stubborn
        self.terminated = self.killed = self.reaped = False
        self.stdout, self.stderr = io.BytesIO(), io.BytesIO()

    def poll(self):
        return self.code

    def terminate(self):
        self.terminated = True
        if not self.stubborn:
            self.code = 0

    def kill(self):
        self.killed = True
        self.code = -9

    def wait(self, timeout=None):
        self.reaped = True
        if self.code is None:
            raise AssertionError("wait called on a running child")
        return self.code


class CleanupTests(unittest.TestCase):
    def test_log_failure_cannot_skip_any_child_reap_or_other_log_close(self):
        harness = content.ContentSoak(SimpleNamespace())
        first, second = OwnedProcess(stubborn=True), OwnedProcess()
        harness.children = [content.Child("fetch", first), content.Child("fetch", second)]
        harness.pump = Mock(side_effect=OSError("disk full"))
        bad_log = Mock()
        bad_log.close.side_effect = OSError("flush failed")
        good_log = Mock()
        harness.logs = {"bad": bad_log, "good": good_log}
        harness.cleanup()
        for process in (first, second):
            self.assertTrue(process.terminated)
            self.assertTrue(process.reaped)
            self.assertTrue(process.stdout.closed)
            self.assertTrue(process.stderr.closed)
        self.assertTrue(first.killed)
        self.assertFalse(second.killed)
        good_log.close.assert_called_once()
        self.assertEqual(harness.counts["failures"], 2)

    def test_failed_shutdown_cannot_preserve_success_summary(self):
        harness = content.ContentSoak(SimpleNamespace(output_dir=Path("/unused")))
        process = OwnedProcess(stubborn=True)
        child = content.Child("daemon", process)
        harness.children = [child]
        harness.daemon = child
        harness.phase = "completed"
        harness.pump = Mock(side_effect=OSError("forced pump failure"))
        harness.cleanup()
        self.assertFalse(harness.summary()["ok"])
        self.assertTrue(process.killed and process.reaped)


if __name__ == "__main__":
    unittest.main()

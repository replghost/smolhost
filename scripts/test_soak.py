#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Offline stdlib tests: python3 -m unittest discover -s scripts -p 'test_soak.py'."""

import json
import contextlib
import io
from types import SimpleNamespace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import soak


def stat_text(start=123, user=350, system=150):
    fields = ["S"] + ["0"] * 21
    fields[11], fields[12], fields[19] = str(user), str(system), str(start)
    return "42 (worker name ) with parens) " + " ".join(fields)


STATUS = "Name:\tdaemon\nVmRSS:\t100 kB\nVmHWM:\t120 kB\nVmSize:\t900 kB\nThreads:\t4\n"


class ProcTests(unittest.TestCase):
    def test_stat_handles_parentheses_and_spaces(self):
        self.assertEqual(soak.parse_stat(stat_text()), (123, 500))

    def test_stat_rejects_invalid_or_negative_counters(self):
        for text in ("missing", "1 (name) S", stat_text(user=-1), stat_text(start=-1)):
            with self.subTest(text=text), self.assertRaises(soak.SoakFailure):
                soak.parse_stat(text)

    def test_status_units_and_required_fields(self):
        self.assertEqual(soak.parse_status(STATUS), {
            "rss_bytes": 102400, "hwm_bytes": 122880, "virtual_bytes": 921600, "threads": 4})
        for text in (STATUS.replace("100 kB", "100 MB"), STATUS.replace("100 kB", "-1 kB"),
                     STATUS.replace("VmRSS:\t100 kB\n", ""), STATUS.replace("Threads:\t4", "Threads:\tno")):
            with self.subTest(text=text), self.assertRaises(soak.SoakFailure):
                soak.parse_status(text)

    def test_large_counters_are_not_truncated(self):
        self.assertEqual(soak.parse_stat(stat_text(user=2**60))[1], 2**60 + 150)
        self.assertEqual(soak.parse_status(STATUS.replace("100 kB", str(2**40) + " kB"))["rss_bytes"], 2**50)

    def test_absent_proc_propagates_failure(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(FileNotFoundError):
                soak.read_proc(42, proc_root=Path(root))

    def test_proc_identity_pss_and_races(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "42"
            directory.mkdir()
            (directory / "stat").write_text(stat_text())
            (directory / "status").write_text(STATUS)
            (directory / "fd").mkdir()
            (directory / "fd" / "0").touch()
            sample = soak.read_proc(42, 123, pss=True, proc_root=Path(root))
            self.assertEqual(sample["fd_count"], 1)
            self.assertIsNone(sample["pss_bytes"])
            self.assertIn("pss_unavailable", sample)
            (directory / "smaps_rollup").write_text("Rss: 100 kB\nPss: 80 kB\n")
            self.assertEqual(soak.read_proc(42, 123, True, Path(root))["pss_bytes"], 81920)
            with self.assertRaisesRegex(soak.SoakFailure, "identity changed"):
                soak.read_proc(42, 124, proc_root=Path(root))
            with patch.object(soak, "parse_stat", side_effect=[(123, 500), (124, 600)]):
                with self.assertRaisesRegex(soak.SoakFailure, "during sample"):
                    soak.read_proc(42, 123, proc_root=Path(root))
            (directory / "status").unlink()
            with self.assertRaises(FileNotFoundError):
                soak.read_proc(42, 123, proc_root=Path(root))


class MetricsTests(unittest.TestCase):
    @staticmethod
    def sample(t, rss, wall=None, cpu=None):
        return {"elapsed_secs": t, "rss_bytes": rss, "hwm_bytes": rss + 100,
                "virtual_bytes": rss * 10, "threads": 4, "fd_count": 10,
                "interval_secs": wall, "cpu_interval_seconds": cpu,
                "cpu_percent": None if wall is None else 100 * cpu / wall}

    def test_empty_and_one_sample_have_no_trend_or_cpu(self):
        metrics = soak.Metrics()
        self.assertIsNone(metrics.summary()["rss_linear_trend_bytes_per_sec"])
        metrics.add(self.sample(100, 500), False)
        result = metrics.summary()
        self.assertEqual(result["rss_first_bytes"], 500)
        self.assertIsNone(result["rss_linear_trend_bytes_per_sec"])
        self.assertIsNone(result["cpu_average_percent"])
        json.dumps(result, allow_nan=False)

    def test_cpu_is_time_weighted_and_can_exceed_one_core(self):
        metrics = soak.Metrics()
        metrics.add(self.sample(100000, 1000), False)
        metrics.add(self.sample(100002, 1200, 2, 4), True)
        metrics.add(self.sample(100010, 2000, 8, 4), True)
        result = metrics.summary()
        self.assertEqual(result["cpu_average_percent"], 80)
        self.assertEqual(result["cpu_peak_interval_percent"], 200)
        self.assertAlmostEqual(result["rss_linear_trend_bytes_per_sec"], 100)
        self.assertEqual(result["rss_observation_span_secs"], 10)
        self.assertEqual(result["rss_peak_bytes"], 2000)
        self.assertEqual(result["lifetime_hwm_peak_bytes"], 2100)

    def test_cross_phase_cpu_interval_is_excluded(self):
        metrics = soak.Metrics()
        metrics.add(self.sample(60, 1000, 10, 50), False)
        metrics.add(self.sample(70, 1000, 10, 1), True)
        self.assertEqual(metrics.summary()["cpu_average_percent"], 10)
        self.assertEqual(metrics.summary()["cpu_measured_interval_secs"], 10)


class EvidenceTests(unittest.TestCase):
    @staticmethod
    def event(kind, number=10, block=None, **extra):
        event = {"event": kind, "genesis_hash": soak.GENESIS,
                 "block_hash": block or ("0x" + f"{number:064x}")}
        if kind != "initialized":
            event["number"] = number
        if kind == "storage":
            event.update(key=soak.NUMBER_KEY, value="0x" + number.to_bytes(4, "little").hex())
        event.update(extra)
        return event

    def initialized(self, target=1):
        evidence = soak.Evidence(soak.GENESIS, target)
        evidence.accept(self.event("initialized"))
        evidence.accept(self.event("storage"))
        return evidence

    def test_full_churn_sequence(self):
        evidence = self.initialized()
        evidence.accept(self.event("storage", 11))
        evidence.accept(self.event("finalized", 11, finalizations=1))
        evidence.accept(self.event("completed", 11, finalizations=1))
        self.assertEqual(evidence.phase, "done")
        self.assertEqual(evidence.counts["storage"], 2)
        self.assertEqual(evidence.counts["finalized"], 1)
        with self.assertRaises(soak.SoakFailure):
            evidence.accept(self.event("completed", 11, finalizations=1))

    def test_initial_snapshot_is_not_finalized_progress(self):
        evidence = self.initialized()
        self.assertEqual(evidence.counts["finalized"], 0)
        with self.assertRaises(soak.SoakFailure):
            evidence.accept(self.event("completed", finalizations=1))

    def test_bad_values_sequences_and_genesis_fail(self):
        bad = [self.event("storage", value=None), self.event("storage", value="0x01000000"),
               self.event("storage", number=True), self.event("storage", key="0x00"),
               self.event("storage", genesis_hash="0x" + "0" * 64),
               self.event("storage", block="0x" + "0" * 64), self.event("finalized", finalizations=1)]
        for event in bad:
            evidence = soak.Evidence(soak.GENESIS, 1)
            evidence.accept(self.event("initialized"))
            with self.subTest(event=event), self.assertRaises(soak.SoakFailure):
                evidence.accept(event)
        for event in (self.event("storage", 10), self.event("storage", 9),
                      self.event("finalized", 11, finalizations=1), self.event("initialized")):
            with self.subTest(event=event), self.assertRaises(soak.SoakFailure):
                self.initialized().accept(event)

    def test_finalization_requires_same_read_and_integer_count(self):
        for extra in ({"finalizations": 2}, {"finalizations": True},
                      {"finalizations": 1, "block_hash": "0x" + "a" * 64}):
            evidence = self.initialized()
            evidence.accept(self.event("storage", 11))
            with self.subTest(extra=extra), self.assertRaises(soak.SoakFailure):
                evidence.accept(self.event("finalized", 11, **extra))

    def test_long_sequence_retains_only_counters_and_last_read(self):
        evidence = self.initialized(0xFFFFFFFF)
        for number in range(11, 1011):
            evidence.accept(self.event("storage", number))
            evidence.accept(self.event("finalized", number, finalizations=number - 10))
        self.assertEqual(evidence.counts["finalized"], 1000)
        self.assertEqual(len(evidence.counts), 3)
        self.assertIsNone(evidence.pending)


class MultiChainTests(unittest.TestCase):
    def test_chain_pins_are_paired_and_unambiguous(self):
        second = "0x" + "a" * 64
        self.assertEqual(soak.chain_targets(["a", "b"], [soak.GENESIS, second]),
                         {"a": soak.GENESIS, "b": second})
        for chains, pins in ((["a", "b"], [soak.GENESIS]), (["a", "a"], [second, second]),
                             ([], []), (["a/b"], [second]), (["a"], ["0x00"]),
                             ([str(n) for n in range(8)], [second] * 8)):
            with self.subTest(chains=chains), self.assertRaises(soak.SoakFailure):
                soak.chain_targets(chains, pins)

    def test_readiness_requires_every_chain_and_progress_stays_separate(self):
        args = SimpleNamespace(chain=["a", "b"], expected_genesis=[soak.GENESIS] * 2,
                               churn_secs=60, duration_secs=180, output_dir=Path("/unused"))
        driver = soak.Soak(args)
        driver.socket = "/unused/rpc.sock"
        driver.progress_file = io.StringIO()
        for chain in args.chain:
            name = "continuous:" + chain
            driver.children[name] = soak.Child(name, "continuous", None, 0,
                                               soak.Evidence(soak.GENESIS, 0xFFFFFFFF), chain=chain)
        def event(chain, kind, number=10, **extra):
            driver.line(driver.children["continuous:" + chain], "stdout",
                        json.dumps(EvidenceTests.event(kind, number, **extra)).encode())
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                event("a", "initialized")
                event("a", "storage")
                self.assertIsNone(driver.ready_at)
                event("b", "initialized")
                self.assertIsNone(driver.ready_at)
                event("b", "storage")
                self.assertIsNotNone(driver.ready_at)
                self.assertEqual(driver.ready_counts, {"a": 0, "b": 0})
                a_progress = driver.last_progress["a"]
                event("b", "storage", 11)
                event("b", "finalized", 11, finalizations=1)
                self.assertEqual(driver.last_progress["a"], a_progress)
                self.assertEqual(driver.children["continuous:a"].evidence.last_number, 10)
                self.assertEqual(driver.children["continuous:b"].evidence.last_number, 11)
        finally:
            driver.selector.close()
            driver.progress_file.close()


class ExitObservationTests(unittest.TestCase):
    def driver(self, phase="done", role="churn"):
        args = SimpleNamespace(chain=["a"], expected_genesis=[soak.GENESIS],
                               churn_secs=60, duration_secs=180, stall_secs=240,
                               output_dir=Path("/unused"))
        driver = soak.Soak(args)
        driver.started = 0
        driver.progress_file = io.StringIO()
        self.addCleanup(driver.selector.close)
        self.addCleanup(driver.progress_file.close)
        process = SimpleNamespace(returncode=None)
        process.poll = lambda: process.returncode
        evidence = soak.Evidence(soak.GENESIS, 1)
        evidence.phase = phase
        child = soak.Child("churn-test", role, process, 100, evidence,
                           chain="a", open_streams=0)
        driver.children[child.name] = child
        return driver, child

    def test_verified_eof_before_exit_is_not_failure_or_early_success(self):
        driver, child = self.driver()
        with patch.object(soak.time, "monotonic", return_value=101):
            driver.check_children()
            self.assertEqual(driver.churn_completed, 0)
            self.assertIn(child.name, driver.children)
            child.process.returncode = 0
            driver.check_children()
        self.assertEqual(driver.churn_completed, 1)
        self.assertNotIn(child.name, driver.children)
        self.assertEqual(driver.exit_codes["churn:0"], 1)

    def test_verified_eof_cannot_hide_a_process_that_never_exits(self):
        driver, child = self.driver()
        with patch.object(soak.time, "monotonic", return_value=101):
            driver.check_children()
        with patch.object(soak.time, "monotonic", return_value=106):
            with self.assertRaisesRegex(soak.SoakFailure, "five seconds"):
                driver.check_children()
        self.assertEqual(driver.churn_completed, 0)

    def test_eof_requires_completion_and_a_successful_exit(self):
        driver, _ = self.driver(phase="storage")
        with self.assertRaisesRegex(soak.SoakFailure, "without verified"):
            driver.check_children()
        driver, child = self.driver()
        with patch.object(soak.time, "monotonic", return_value=101):
            driver.check_children()
            child.process.returncode = 1
            with self.assertRaisesRegex(soak.SoakFailure, "code 1"):
                driver.check_children()
        self.assertEqual(driver.churn_completed, 0)

    def test_continuous_processes_do_not_get_churn_eof_grace(self):
        for role in ("daemon", "continuous"):
            with self.subTest(role=role):
                driver, _ = self.driver(role=role)
                with self.assertRaisesRegex(soak.SoakFailure, "without verified"):
                    driver.check_children()


class OutputTests(unittest.TestCase):
    def test_tail_logs_rotate_at_fixed_bound(self):
        with tempfile.TemporaryDirectory() as root, patch.object(soak, "LOG_BYTES", 64):
            path = Path(root) / "daemon.log"
            log = soak.TailLog(path)
            try:
                for number in range(100):
                    log.append(f"entry {number}".encode())
            finally:
                log.close()
            self.assertIn(b"entry 99\n", path.read_bytes())
            files = list(Path(root).iterdir())
            self.assertEqual(len(files), 2)
            self.assertTrue(all(file.stat().st_size <= 64 for file in files))

    def test_optional_limits_reject_nan_and_infinity(self):
        for text in ("nan", "inf", "-inf", "0", "-1"):
            with self.subTest(text=text), self.assertRaises(soak.argparse.ArgumentTypeError):
                soak.positive_number(text)


if __name__ == "__main__":
    unittest.main()

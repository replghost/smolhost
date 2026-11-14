#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Linux read-only soak using already-built binaries; evidence is private, not publishable.

The duration starts after every monitored chain's first verified System.Number read.
A new, empty checkpoint directory makes this a cold isolated run, NOT --no-state.
No retries, reconnects, service changes, transactions, or remote RPC fallback.
"""

import argparse
from collections import Counter
from dataclasses import dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parent.parent
GENESIS = "0xd6eec26135305a8ad257a20d003357284c8aa03d0bdb2b357ab0a22371e11ef2"
NUMBER_KEY = "0x26aa394eea5630e07c48ae0c9558cef702a5c1b19ab7a04f536c519aca4983ac"
HASH = re.compile(r"0x[0-9a-fA-F]{64}\Z")
VALUE = re.compile(r"0x[0-9a-fA-F]{8}\Z")
CHAIN_ID = re.compile(r"[A-Za-z0-9_-]+\Z")
MAX_LINE = 1024 * 1024
LOG_BYTES = 1024 * 1024


class SoakFailure(Exception):
    pass


def emit(file, record):
    file.write(json.dumps(record, sort_keys=True, allow_nan=False) + "\n")
    file.flush()


def identity(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
        info = os.fstat(source.fileno())
    return {"path": str(path), "sha256": digest.hexdigest(), "bytes": info.st_size,
            "device": info.st_dev, "inode": info.st_ino, "mtime_ns": info.st_mtime_ns}


def parse_stat(text):
    # comm may contain spaces and parentheses; numeric fields follow its LAST ')'.
    end = text.rfind(")")
    if end < 0:
        raise SoakFailure("malformed /proc stat")
    fields = text[end + 1:].split()
    try:
        user, system = int(fields[11]), int(fields[12])
        start = int(fields[19])
    except (IndexError, ValueError):
        raise SoakFailure("malformed /proc stat counters") from None
    if min(user, system, start) < 0:
        raise SoakFailure("negative /proc stat counters")
    return start, user + system


def parse_status(text):
    wanted = {"VmRSS": "rss_bytes", "VmHWM": "hwm_bytes", "VmSize": "virtual_bytes",
              "Threads": "threads"}
    result = {}
    for line in text.splitlines():
        key, separator, raw = line.partition(":")
        if not separator or key not in wanted:
            continue
        fields = raw.split()
        try:
            number = int(fields[0])
        except (IndexError, ValueError):
            raise SoakFailure("malformed /proc status counter: " + key) from None
        units = [] if key == "Threads" else ["kB"]
        if fields[1:] != units or number < 0:
            raise SoakFailure("invalid /proc status units/value: " + key)
        result[wanted[key]] = number if key == "Threads" else number * 1024
    if result.keys() != set(wanted.values()):
        raise SoakFailure("missing /proc status counters (daemon may have exited)")
    return result


def read_proc(pid, expected_start=None, pss=False, proc_root=Path("/proc")):
    directory = proc_root / str(pid)
    before, _ = parse_stat((directory / "stat").read_text())
    if expected_start is not None and before != expected_start:
        raise SoakFailure("daemon PID identity changed")
    result = parse_status((directory / "status").read_text())
    with os.scandir(directory / "fd") as entries:
        result["fd_count"] = sum(1 for _ in entries)
    result["pss_bytes"] = None
    if pss:
        try:
            with (directory / "smaps_rollup").open() as source:
                for line in source:
                    if line.startswith("Pss:"):
                        parts = line.split()
                        if len(parts) != 3 or parts[2] != "kB" or int(parts[1]) < 0:
                            raise SoakFailure("invalid /proc Pss value")
                        result["pss_bytes"] = int(parts[1]) * 1024
                        break
            if result["pss_bytes"] is None:
                result["pss_unavailable"] = "Pss field absent"
        except OSError as error:
            # PSS is optional: ptrace restrictions can deny it even for our child.
            result["pss_unavailable"] = type(error).__name__
    after, ticks = parse_stat((directory / "stat").read_text())
    if after != before:
        raise SoakFailure("daemon PID identity changed during sample")
    result.update(starttime_ticks=after, cpu_ticks=ticks)
    return result


class Metrics:
    """Constant-memory RSS regression and time-weighted CPU interval aggregation."""

    def __init__(self):
        self.count = 0
        self.first_time = self.last_time = None
        self.first_rss = self.last_rss = self.minimum = self.peak = None
        self.mean_t = self.mean_rss = self.m2_t = self.cross = 0.0
        self.cpu_seconds = self.cpu_wall = self.cpu_peak = 0.0
        self.hwm_peak = self.virtual_peak = self.threads_peak = self.fd_peak = None

    def add(self, sample, include_cpu):
        t, rss = sample["elapsed_secs"], sample["rss_bytes"]
        self.count += 1
        if self.first_time is None:
            self.first_time, self.first_rss = t, rss
            self.minimum = self.peak = rss
        self.last_time, self.last_rss = t, rss
        self.minimum, self.peak = min(self.minimum, rss), max(self.peak, rss)
        for field, key in (("hwm_peak", "hwm_bytes"), ("virtual_peak", "virtual_bytes"),
                           ("threads_peak", "threads"), ("fd_peak", "fd_count")):
            value = sample[key]
            previous = getattr(self, field)
            setattr(self, field, value if previous is None else max(previous, value))
        dt, dr = t - self.mean_t, rss - self.mean_rss
        self.mean_t += dt / self.count
        self.mean_rss += dr / self.count
        self.m2_t += dt * (t - self.mean_t)
        self.cross += dt * (rss - self.mean_rss)
        if include_cpu and sample["cpu_percent"] is not None:
            self.cpu_seconds += sample["cpu_interval_seconds"]
            self.cpu_wall += sample["interval_secs"]
            self.cpu_peak = max(self.cpu_peak, sample["cpu_percent"])

    def summary(self):
        span = None if not self.count else self.last_time - self.first_time
        return {"samples": self.count, "first_elapsed_secs": self.first_time,
                "last_elapsed_secs": self.last_time, "rss_observation_span_secs": span,
                "rss_first_bytes": self.first_rss, "rss_last_bytes": self.last_rss,
                "rss_min_bytes": self.minimum, "rss_peak_bytes": self.peak,
                "lifetime_hwm_peak_bytes": self.hwm_peak, "virtual_peak_bytes": self.virtual_peak,
                "threads_peak": self.threads_peak, "fd_peak": self.fd_peak,
                "rss_linear_trend_bytes_per_sec": self.cross / self.m2_t if self.m2_t > 0 else None,
                "cpu_measured_interval_secs": self.cpu_wall,
                "cpu_average_percent": 100 * self.cpu_seconds / self.cpu_wall if self.cpu_wall else None,
                "cpu_peak_interval_percent": self.cpu_peak if self.cpu_wall else None}


class TailLog:
    """At most two LOG_BYTES files per role, regardless of session churn."""

    def __init__(self, path):
        self.path = path
        self.file = path.open("xb")
        self.size = 0

    def append(self, raw):
        raw = raw[-(LOG_BYTES - 1):] + b"\n"
        if self.size + len(raw) > LOG_BYTES:
            self.file.close()
            self.path.replace(self.path.with_suffix(self.path.suffix + ".1"))
            self.file = self.path.open("xb")
            self.size = 0
        self.file.write(raw)
        self.file.flush()
        self.size += len(raw)

    def close(self):
        self.file.close()


@dataclass
class Evidence:
    expected_genesis: str
    target: int
    counts: Counter = field(default_factory=Counter)
    phase: str = "initialized"
    initial_block: str = None
    last_block: str = None
    last_number: int = None
    pending: tuple = None

    def accept(self, event):
        if not isinstance(event, dict):
            raise SoakFailure("probe emitted a non-object event")
        kind, block, genesis = (event.get(key) for key in ("event", "block_hash", "genesis_hash"))
        if kind not in ("initialized", "storage", "finalized", "completed"):
            raise SoakFailure("probe emitted an unexpected event")
        if not isinstance(block, str) or not HASH.fullmatch(block):
            raise SoakFailure("probe emitted an invalid block hash")
        if not isinstance(genesis, str) or genesis.lower() != self.expected_genesis:
            raise SoakFailure("probe genesis does not match the configured pin")
        block = block.lower()
        if kind == "initialized":
            if self.phase != "initialized":
                raise SoakFailure("duplicate probe initialization")
            self.initial_block, self.phase = block, "initial_storage"
        else:
            number = event.get("number")
            if type(number) is not int or not 0 <= number <= 0xFFFFFFFF:
                raise SoakFailure("probe returned invalid numeric block storage")
            if kind == "storage":
                value = event.get("value")
                if (not isinstance(value, str) or not VALUE.fullmatch(value)
                        or event.get("key") != NUMBER_KEY
                        or int.from_bytes(bytes.fromhex(value[2:]), "little") != number):
                    raise SoakFailure("probe returned invalid System.Number storage")
                if self.phase == "initial_storage":
                    if block != self.initial_block:
                        raise SoakFailure("initial storage does not match initialization")
                    self.last_number, self.last_block = number, block
                    self.phase = "storage"
                elif self.phase == "storage" and number > self.last_number and block != self.last_block:
                    self.pending, self.phase = (block, number), "finalized"
                else:
                    raise SoakFailure("storage sequence invalid or block number did not advance")
            elif kind == "finalized":
                if (self.phase != "finalized" or self.pending != (block, number)
                        or type(event.get("finalizations")) is not int
                        or event["finalizations"] != self.counts["finalized"] + 1):
                    raise SoakFailure("finalization lacks matching storage or sequential count")
                self.last_block, self.last_number = self.pending
                self.pending = None
                self.phase = "completed" if event["finalizations"] == self.target else "storage"
            elif (self.phase != "completed" or (block, number) != (self.last_block, self.last_number)
                  or type(event.get("finalizations")) is not int
                  or event["finalizations"] != self.target):
                raise SoakFailure("probe completion evidence invalid")
            else:
                self.phase = "done"
        self.counts[kind] += 1
        return kind


@dataclass
class Child:
    name: str
    role: str
    process: subprocess.Popen
    started: float
    evidence: Evidence = None
    chain: str = None
    open_streams: int = 2
    stop_requested: bool = False
    recorded: bool = False


class Soak:
    def __init__(self, args):
        self.args = args
        self.started = time.monotonic()
        self.wall_started = time.time()
        self.ready_at = self.workload_end = None
        self.targets = chain_targets(args.chain, args.expected_genesis)
        self.last_progress = {}
        self.ready_counts = {}
        self.probes_started = False
        self.selector = selectors.DefaultSelector()
        self.buffers = {}
        self.children = {}
        self.logs = {}
        self.files = []
        self.samples_file = self.progress_file = None
        self.listening = self.stopping = False
        self.daemon_stopped = False
        self.shutdown_checkpoint = False
        self.checkpoint_count = 0
        self.starttime = self.previous_sample = None
        self.next_pss = self.next_sample = 0
        self.next_churn = math.inf
        self.next_status = 0
        self.metrics = {key: Metrics() for key in ("all", "startup", "workload", "steady")}
        self.events = Counter()
        self.churn_started = self.churn_completed = self.churn_cancelled = 0
        self.exit_codes = Counter()
        self.failure = None
        self.metadata = {}
        self.clock_ticks = os.sysconf("SC_CLK_TCK")

    def elapsed(self):
        return time.monotonic() - self.started

    def progress(self, record):
        emit(self.progress_file, {"elapsed_secs": self.elapsed(), **record})

    def prepare(self, socket):
        self.socket = socket
        args = self.args
        for name in ("samples.jsonl", "progress.jsonl"):
            file = (args.output_dir / name).open("x", encoding="utf-8")
            self.files.append(file)
        self.samples_file, self.progress_file = self.files
        for role in ("daemon", "continuous", "churn"):
            self.logs[role] = TailLog(args.output_dir / (role + ".log"))
        binaries = {}
        for name in ("smolhostd", "smolhost"):
            path = args.bin_dir / name
            if not path.is_file() or not os.access(path, os.X_OK):
                raise SoakFailure("already-built executable unavailable: " + str(path))
            binaries[name] = identity(path)
        specs, ids = [], []
        for path in (args.chain_spec, *args.parachain_spec):
            specification = json.loads(path.read_text(encoding="utf-8"))
            route = specification.get("id") if isinstance(specification, dict) else None
            if not isinstance(route, str) or not CHAIN_ID.fullmatch(route) or route in ids:
                raise SoakFailure("chain specification has an invalid or duplicate route ID")
            specs.append({"route": route, **identity(path)})
            ids.append(route)
        if any(chain not in ids for chain in self.targets):
            raise SoakFailure("requested route is not configured")
        (args.output_dir / "state").mkdir(mode=0o700)
        profile = args.binary_profile
        if profile is None:
            profile = args.bin_dir.name if args.bin_dir.name in ("debug", "release") else "unknown"
        self.metadata = {"schema_version": 2, "started_unix_secs": self.wall_started,
                         "socket": socket, "clock_ticks_per_second": self.clock_ticks,
                         "platform": list(os.uname()), "python": sys.version,
                         "binaries": binaries, "binary_profile_label": profile,
                         "binary_profile_provenance": "user label" if args.binary_profile else "path inference only",
                         "chain_specs": specs, "options": {key: str(value) if isinstance(value, Path)
                         else [str(item) for item in value] if isinstance(value, list) else value
                         for key, value in vars(args).items()},
                         "state_mode": "isolated initially empty (cold); periodic and shutdown saves enabled",
                         "cpu_definition": "daemon user+system / monotonic interval; 100% = one core",
                         "rss_trend_note": "OLS over samples, not a leak diagnosis; steady excludes workload warmup",
                         "privacy": "Local evidence includes host paths, peer logs and checkpoint data; do not publish"}
        with (args.output_dir / "metadata.json").open("x", encoding="utf-8") as file:
            emit(file, self.metadata)

    def start(self, name, role, command, target=None, chain=None):
        environment = os.environ.copy()
        environment.update(RUST_LOG="info", RUST_LOG_STYLE="never")
        for key in ("LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES", "NOTIFY_SOCKET"):
            environment.pop(key, None)
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=environment, start_new_session=True)
        child = Child(name, role, process, time.monotonic(),
                      Evidence(self.targets[chain], target) if target else None, chain=chain)
        self.children[name] = child
        for channel in ("stdout", "stderr"):
            stream = getattr(process, channel)
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, (child, channel))
            self.buffers[stream.fileno()] = bytearray()
        self.progress({"event": "process_started", "child": name, "pid": process.pid, "command": command})
        return child

    def probe(self, name, chain, target, timeout):
        return self.start(name, "continuous" if name.startswith("continuous:") else "churn",
                          [str(self.args.bin_dir / "smolhost"), "--socket", self.socket,
                           "--chain", chain, "probe", "--finalizations", str(target),
                           "--timeout-secs", str(timeout)], target, chain)

    def line(self, child, channel, raw):
        if child.role == "daemon" or channel == "stderr":
            self.logs[child.role].append(raw)
            text = raw.decode("utf-8", errors="replace")
            if child.role != "daemon":
                if text.strip():
                    raise SoakFailure(child.name + " emitted stderr; see bounded role log")
                return
            if "host: listening socket=" in text:
                self.listening = True
            if "host: checkpoint saved " in text:
                self.checkpoint_count += 1
                if child.stop_requested:
                    self.shutdown_checkpoint = True
            if "host: stopped" in text:
                self.daemon_stopped = True
            if re.search(r"\bERROR\b|^Error:|panicked at", text):
                raise SoakFailure("daemon reported an error; see daemon.log")
            return
        try:
            event = json.loads(raw)
        except (ValueError, UnicodeError, RecursionError):
            raise SoakFailure(child.name + " emitted invalid JSONL") from None
        kind = child.evidence.accept(event)
        self.events[kind] += 1
        self.progress({"child": child.name, "chain": child.chain, **event})
        if child.role == "continuous":
            now = time.monotonic()
            if kind == "storage" and child.chain not in self.last_progress:
                self.last_progress[child.chain] = now
                if len(self.last_progress) == len(self.targets):
                    self.ready_at = now
                    self.ready_counts = {
                        chain: self.children["continuous:" + chain].evidence.counts["finalized"]
                        for chain in self.targets
                    }
                    self.next_churn = now + self.args.churn_secs if self.args.churn_secs else math.inf
                    self.next_sample = now
                    self.progress({"event": "ready", "duration_secs": self.args.duration_secs})
                    print("SOAK_READY " + json.dumps({"socket": self.socket,
                          "chains": {chain: self.children["continuous:" + chain].evidence.last_number
                                     for chain in self.targets},
                          "output_dir": str(self.args.output_dir)}), flush=True)
            elif kind == "finalized":
                self.last_progress[child.chain] = now
            elif kind == "completed":
                raise SoakFailure("continuous probe unexpectedly completed")

    def pump(self, timeout=0.1, cleanup=False):
        for key, _ in self.selector.select(timeout):
            child, channel = key.data
            stream = key.fileobj
            fd = stream.fileno()
            try:
                chunk = os.read(fd, 65536)
            except BlockingIOError:
                continue
            buffer = self.buffers[fd]
            buffer.extend(chunk)
            while b"\n" in buffer:
                raw, _, rest = buffer.partition(b"\n")
                buffer[:] = rest
                try:
                    if len(raw) > MAX_LINE:
                        raise SoakFailure(child.name + " exceeded log line bound")
                    self.line(child, channel, raw)
                except SoakFailure as error:
                    if not cleanup:
                        raise
                    self.note_failure(str(error))
            if len(buffer) > MAX_LINE:
                buffer.clear()
                if not cleanup:
                    raise SoakFailure(child.name + " exceeded unterminated line bound")
                self.note_failure(child.name + " exceeded unterminated line bound")
            if not chunk:
                trailing = bytes(buffer)
                del self.buffers[fd]
                self.selector.unregister(stream)
                stream.close()
                child.open_streams -= 1
                if trailing:
                    if child.role != "daemon" and channel == "stdout":
                        if not cleanup:
                            raise SoakFailure(child.name + " emitted truncated JSONL")
                        self.note_failure(child.name + " emitted truncated JSONL")
                    else:
                        try:
                            self.line(child, channel, trailing)
                        except SoakFailure as error:
                            if not cleanup:
                                raise
                            self.note_failure(str(error))

    def record_exit(self, child):
        if child.recorded:
            return
        child.recorded = True
        code = child.process.returncode
        self.exit_codes[child.role + ":" + str(code)] += 1
        self.progress({"event": "process_exited", "child": child.name, "exit_code": code,
                       "stop_requested": child.stop_requested,
                       "counts": dict(child.evidence.counts) if child.evidence else {}})

    def check_children(self):
        for child in list(self.children.values()):
            code = child.process.poll()
            if code is None:
                if child.open_streams == 0:
                    raise SoakFailure(child.name + " closed output while still running")
                if child.role == "churn" and time.monotonic() - child.started > self.args.stall_secs:
                    raise SoakFailure(child.name + " exceeded churn deadline")
                continue
            if child.role != "churn" or code != 0:
                raise SoakFailure(child.name + " exited prematurely with code " + str(code))
            if child.open_streams:
                continue
            if child.evidence.phase != "done":
                raise SoakFailure(child.name + " exited without verified completion")
            self.churn_completed += 1
            self.record_exit(child)
            del self.children[child.name]

    def sample(self):
        daemon = self.children["daemon"]
        began = self.elapsed()
        sample = read_proc(daemon.process.pid, self.starttime, began >= self.next_pss)
        now = self.elapsed()
        if began >= self.next_pss:
            self.next_pss = now + self.args.pss_secs if self.args.pss_secs else math.inf
        self.starttime = sample["starttime_ticks"]
        ready_elapsed = None if self.ready_at is None else self.ready_at - self.started
        phase = "startup" if ready_elapsed is None else "workload"
        steady = ready_elapsed is not None and now - ready_elapsed >= self.args.warmup_secs
        sample.update(elapsed_secs=now, sample_read_secs=now - began, phase=phase, steady=steady,
                      pid=daemon.process.pid, interval_secs=None, cpu_interval_seconds=None,
                      cpu_percent=None)
        previous = self.previous_sample
        if previous is not None:
            interval = now - previous["elapsed_secs"]
            ticks = sample["cpu_ticks"] - previous["cpu_ticks"]
            if interval <= 0 or ticks < 0:
                raise SoakFailure("invalid process CPU sample interval/counter")
            cpu = ticks / self.clock_ticks
            sample.update(interval_secs=interval, cpu_interval_seconds=cpu, cpu_percent=100 * cpu / interval)
        self.metrics["all"].add(sample, True)
        self.metrics[phase].add(sample, previous is not None and previous["phase"] == phase)
        if steady:
            self.metrics["steady"].add(sample, previous is not None and previous["steady"])
        self.previous_sample = sample
        emit(self.samples_file, sample)
        self.next_sample = time.monotonic() + self.args.sample_secs
        if self.args.max_rss_mib is not None and sample["rss_bytes"] > self.args.max_rss_mib * 1024 * 1024:
            raise SoakFailure("optional RSS limit exceeded")
        if (self.args.max_cpu_percent is not None and sample["cpu_percent"] is not None
                and sample["cpu_percent"] > self.args.max_cpu_percent):
            raise SoakFailure("optional CPU interval limit exceeded")

    def run(self, socket):
        self.socket = socket
        command = [str(self.args.bin_dir / "smolhostd"), "--chain-spec", str(self.args.chain_spec),
                   "--socket", socket, "--state-dir", str(self.args.output_dir / "state")]
        for path in self.args.parachain_spec:
            command += ["--parachain-spec", str(path)]
        self.start("daemon", "daemon", command)
        self.next_pss = 0 if self.args.pss_secs else math.inf
        while True:
            self.pump()
            self.check_children()
            now = time.monotonic()
            if self.listening and not self.probes_started:
                total = self.args.startup_timeout_secs + self.args.duration_secs + self.args.stall_secs + 60
                for chain in self.targets:
                    self.probe("continuous:" + chain, chain, 0xFFFFFFFF, total)
                self.probes_started = True
            for chain, last in self.last_progress.items():
                if now - last >= self.args.stall_secs:
                    raise SoakFailure("continuous finalized storage progress stalled: " + chain)
            if self.ready_at is None:
                if now - self.started >= self.args.startup_timeout_secs:
                    missing = sorted(set(self.targets) - self.last_progress.keys())
                    raise SoakFailure("startup deadline: no verified storage read for " + ", ".join(missing))
            else:
                if now - self.ready_at >= self.args.duration_secs:
                    self.workload_end = now
                    for chain, before in self.ready_counts.items():
                        if self.children["continuous:" + chain].evidence.counts["finalized"] <= before:
                            raise SoakFailure("duration ended without advancing finalized storage: " + chain)
                    self.sample()
                    return
                if now >= self.next_churn and not any(c.role == "churn" for c in self.children.values()):
                    chain = list(self.targets)[self.churn_started % len(self.targets)]
                    self.churn_started += 1
                    self.probe("churn-" + str(self.churn_started), chain, 1, self.args.stall_secs)
                    self.next_churn = now + self.args.churn_secs
            if now >= self.next_sample:
                self.sample()
            if now >= self.next_status:
                phase = "startup" if self.ready_at is None else "workload"
                print(f"SOAK_PROGRESS phase={phase} elapsed={self.elapsed():.1f}s "
                      f"reads={self.events['storage']} finalized={self.events['finalized']} "
                      f"churn_completed={self.churn_completed}", flush=True)
                self.next_status = now + 60

    def note_failure(self, reason):
        if self.failure is None:
            self.failure = reason

    def stop_group(self, children, timeout):
        for child in children:
            if child.process.poll() is None:
                child.stop_requested = True
                try:
                    child.process.terminate()
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + timeout
        while any(c.process.poll() is None or c.open_streams for c in children):
            if time.monotonic() >= deadline:
                break
            try:
                self.pump(cleanup=True)
            except (OSError, ValueError) as error:
                self.note_failure("cleanup stream error: " + str(error))
                break
        for child in children:
            if child.process.poll() is None:
                self.note_failure(child.name + " did not stop within shutdown deadline; killed")
                try:
                    child.process.kill()
                except ProcessLookupError:
                    pass
            child.process.wait()
        # After wait(), pipes cannot receive new bytes from these binaries.
        drain_deadline = time.monotonic() + 2
        while any(c.open_streams for c in children) and time.monotonic() < drain_deadline:
            try:
                self.pump(timeout=0.05, cleanup=True)
            except (OSError, ValueError) as error:
                self.note_failure("cleanup drain error: " + str(error))
                break
        for child in children:
            if child.open_streams:
                self.note_failure(child.name + " output did not reach EOF during cleanup")
            code = child.process.returncode
            if child.role == "daemon":
                if code != 0:
                    self.note_failure("daemon shutdown exit code " + str(code))
            elif child.role == "churn" and code == 0 and child.evidence.phase == "done":
                self.churn_completed += 1
            elif child.stop_requested and code == -signal.SIGTERM:
                if child.role == "churn":
                    self.churn_cancelled += 1
            else:
                self.note_failure(child.name + " unexpected shutdown exit code " + str(code))
            self.record_exit(child)

    def cleanup(self):
        self.stopping = True
        if self.workload_end is None:
            self.workload_end = time.monotonic()
        clients = [c for c in self.children.values() if c.role != "daemon"]
        self.stop_group(clients, 5)
        daemon = self.children.get("daemon")
        if daemon is not None:
            # Drain pre-stop logs before using the save marker as shutdown evidence.
            self.pump(timeout=0, cleanup=True)
            self.stop_group([daemon], 20)
            if not self.daemon_stopped or not self.shutdown_checkpoint:
                self.note_failure("missing clean daemon stop/shutdown checkpoint evidence")
            checkpoints = list((self.args.output_dir / "state").glob("*.json"))
            if len(checkpoints) != 1 or checkpoints[0].stat().st_size == 0:
                self.note_failure("missing or unexpected saved relay checkpoint")

    def close(self):
        self.selector.close()
        for child in self.children.values():
            for channel in ("stdout", "stderr"):
                getattr(child.process, channel).close()
        for file in self.files:
            file.close()
        for log in self.logs.values():
            log.close()

    def summary(self):
        continuous = {
            child.chain: {"counts": dict(child.evidence.counts),
                          "last_finalized_number": child.evidence.last_number,
                          "exit_code": child.process.returncode}
            for child in self.children.values() if child.role == "continuous"
        }
        return {"schema_version": 2, "ok": self.failure is None,
                "reason": self.failure or "requested workload duration completed",
                "output_dir": str(self.args.output_dir), "elapsed_secs": self.elapsed(),
                "requested_workload_secs": self.args.duration_secs,
                "startup_secs": None if self.ready_at is None else self.ready_at - self.started,
                "observed_workload_secs": 0 if self.ready_at is None else self.workload_end - self.ready_at,
                "counts": dict(self.events),
                "continuous": continuous,
                "churn_started": self.churn_started, "churn_completed": self.churn_completed,
                "churn_cancelled_at_stop": self.churn_cancelled, "exit_codes": dict(self.exit_codes),
                "daemon_exit_code": self.children["daemon"].process.returncode if "daemon" in self.children else None,
                "shutdown_checkpoint_observed": self.shutdown_checkpoint,
                "checkpoint_save_markers": self.checkpoint_count,
                "metrics": {name: metric.summary() for name, metric in self.metrics.items()},
                "steady_warmup_secs_after_first_read": self.args.warmup_secs,
                "limits": "RSS/CPU are daemon-only samples, not allocator leaks or system/network attribution; "
                          "VmHWM is lifetime, PSS optional, startup/workload/steady intervals separate; "
                          "duration excludes startup and shutdown; interrupted runs fail; "
                          "continuous probe SIGTERM at requested stop is expected, not completed probe success"}


def positive_integer(value):
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive integer") from None
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def nonnegative_integer(value):
    if value == "0":
        return 0
    return positive_integer(value)


def positive_number(value):
    try:
        number = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a finite positive number") from None
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return number


def chain_targets(chains, genesis_hashes):
    if not 1 <= len(chains) <= 7 or len(chains) != len(genesis_hashes):
        raise SoakFailure("provide one --expected-genesis per --chain, with 1–7 chains")
    if len(set(chains)) != len(chains):
        raise SoakFailure("duplicate monitored chain")
    if (any(not CHAIN_ID.fullmatch(chain) for chain in chains)
            or any(not HASH.fullmatch(genesis) for genesis in genesis_hashes)):
        raise SoakFailure("invalid chain route or expected genesis hash")
    return dict(zip(chains, (genesis.lower() for genesis in genesis_hashes)))


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True, help="NEW private evidence directory, outside Cargo target")
    parser.add_argument("--bin-dir", type=Path, default=ROOT / "target/debug")
    parser.add_argument("--binary-profile", help="optional build profile label; hashes, not labels, identify binaries")
    parser.add_argument("--duration-secs", type=positive_integer, default=21600,
                        help="workload seconds AFTER every monitored chain's first verified read (default: 21600)")
    parser.add_argument("--sample-secs", type=positive_integer, default=10)
    parser.add_argument("--stall-secs", type=positive_integer, default=240)
    parser.add_argument("--startup-timeout-secs", type=positive_integer, default=300)
    parser.add_argument("--churn-secs", type=nonnegative_integer, default=60, help="start a one-finalization probe periodically, rotating chains; 0 disables")
    parser.add_argument("--pss-secs", type=nonnegative_integer, default=60, help="optional PSS sampling cadence; 0 disables")
    parser.add_argument("--warmup-secs", type=nonnegative_integer, default=60, help="exclude initial workload seconds from steady RSS trend")
    parser.add_argument("--max-rss-mib", type=positive_number, help="optional instantaneous sampled RSS failure limit")
    parser.add_argument("--max-cpu-percent", type=positive_number, help="optional sampled CPU failure limit; 100%% is one core")
    parser.add_argument("--chain-spec", type=Path, default=ROOT / "chains/paseo.json")
    parser.add_argument("--parachain-spec", type=Path, action="append", help="repeatable; replaces bundled Asset Hub spec")
    parser.add_argument("--chain", action="append", help="monitored route; repeat for simultaneous chains (default: asset-hub-paseo)")
    parser.add_argument("--expected-genesis", action="append", help="one pinned genesis per --chain, in the same order (default: classic Asset Hub Paseo)")
    args = parser.parse_args()
    args.chain = args.chain if args.chain is not None else ["asset-hub-paseo"]
    args.expected_genesis = args.expected_genesis if args.expected_genesis is not None else [GENESIS]
    try:
        chain_targets(args.chain, args.expected_genesis)
    except SoakFailure as error:
        parser.error(str(error))
    if args.parachain_spec is None:
        args.parachain_spec = [ROOT / "chains/paseo_asset_hub.json"]
    for name in ("bin_dir", "chain_spec", "output_dir"):
        setattr(args, name, getattr(args, name).expanduser().absolute())
    args.bin_dir = args.bin_dir.resolve()
    args.parachain_spec = [path.expanduser().resolve() for path in args.parachain_spec]
    target_dirs = [(ROOT / "target").resolve()]
    if os.environ.get("CARGO_TARGET_DIR"):
        target_dirs.append(Path(os.environ["CARGO_TARGET_DIR"]).expanduser().resolve())
    resolved = args.output_dir.resolve()
    if "target" in resolved.parts or any(resolved == target or target in resolved.parents for target in target_dirs):
        parser.error("evidence/checkpoint output must be outside Cargo target directories")
    if sys.platform != "linux":
        parser.error("Linux /proc is required")
    return args


def main():
    args = arguments()
    os.umask(0o077)
    # mkdir is the exclusive reservation. Never open an existing evidence directory,
    # including an existing symlink, even if it appears empty.
    try:
        args.output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    except OSError as error:
        print(json.dumps({"ok": False, "reason": "cannot create new output directory: " + str(error)}), flush=True)
        return 1
    soak = Soak(args)
    temporary = None

    def interrupted(signum, _frame):
        raise KeyboardInterrupt("signal " + str(signum))

    previous_int = signal.signal(signal.SIGINT, interrupted)
    previous_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        temporary = tempfile.TemporaryDirectory(prefix="sh-soak-", dir="/tmp")
        os.chmod(temporary.name, 0o700)
        soak.prepare(str(Path(temporary.name) / "rpc.sock"))
        soak.run(str(Path(temporary.name) / "rpc.sock"))
    except KeyboardInterrupt as error:
        soak.note_failure("interrupted: " + str(error))
    except Exception as error:
        soak.note_failure(type(error).__name__ + ": " + str(error))
    finally:
        # A second signal must not strand our own children during bounded shutdown.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            soak.cleanup()
        except Exception as error:
            soak.note_failure("cleanup error: " + type(error).__name__ + ": " + str(error))
        finally:
            # Evidence I/O failure must not bypass SIGTERM/checkpoint shutdown.
            remaining = [child for child in soak.children.values() if child.process.poll() is None]
            for child in remaining:
                try:
                    child.process.terminate()
                except ProcessLookupError:
                    pass
            deadline = time.monotonic() + 20
            for child in soak.children.values():
                try:
                    child.process.wait(timeout=max(0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    child.process.kill()
                    child.process.wait()
            try:
                soak.close()
                if temporary is not None:
                    temporary.cleanup()
            except OSError as error:
                soak.note_failure("closing evidence/runtime failed: " + str(error))
            signal.signal(signal.SIGINT, previous_int)
            signal.signal(signal.SIGTERM, previous_term)
    summary = soak.summary()
    try:
        with (args.output_dir / "summary.json").open("x", encoding="utf-8") as file:
            emit(file, summary)
            os.fsync(file.fileno())
    except OSError as error:
        summary.update(ok=False, reason="summary persistence failed: " + str(error), prior_reason=summary["reason"])
    print(json.dumps(summary, sort_keys=True, allow_nan=False), flush=True)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

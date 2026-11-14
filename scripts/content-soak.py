#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Opt-in Linux content soak; uses built binaries, public peers, and PRIVATE evidence.

No service is contacted or installed. Duration is the workload window after the
Bulletin finalized probe (startup has a separate 300s limit). A new pair is not
started during the final 180s, so outstanding downloads never become expected
shutdown cancellations. Logs rotate; completed verified files are removed.
"""

import argparse
import base64
from collections import Counter
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import time

import soak

ROOT = Path(__file__).resolve().parent.parent
CHAIN = "next-bulletin-paseo"
GENESIS = "0x8cfe6717dc4becfda2e13c488a1e2061ff2dfee96e7d031157f72d36716c0a22"
SPECS = ("paseo.json", "paseo_next_asset_hub.json", "paseo_next_bulletin.json",
         "paseo_next_individuality.json")
MAX_BYTES = 64 * 1024 * 1024
Failure = soak.SoakFailure


@dataclass(frozen=True)
class Fixture:
    cid: str
    sha256: str
    bytes: int
    blocks: int

    def receipt(self):
        return {"cid": self.cid, "sha256": self.sha256,
                "bytes": self.bytes, "blocks": self.blocks}


def verify_download(path, stdout, fixture):
    """Never delete a download unless BOTH the actual file and receipt agree."""
    try:
        receipt = json.loads(stdout)
    except (ValueError, UnicodeError):
        raise Failure("download receipt is not one JSON object") from None
    if (not isinstance(receipt, dict) or receipt != fixture.receipt()
            or type(receipt.get("bytes")) is not int
            or type(receipt.get("blocks")) is not int):
        raise Failure("download receipt differs from independently pinned fixture")
    # No symlink following, including on the check/open boundary.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size != fixture.bytes:
            raise Failure("download is not a regular file of the pinned size")
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
        if digest.hexdigest() != fixture.sha256:
            raise Failure("download SHA256 differs from independent pin")
    current = path.lstat()
    if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
        raise Failure("download identity changed before deletion")
    path.unlink()
    return receipt


class WebSocket:
    """Small bounded RFC6455 client; reads no bytes ahead of the requested frame.

    In particular there is no library/background reader draining the adversarial
    socket while the main loop measures the daemon's blocked-writer timeout.
    """

    def __init__(self, path, pump, deadline):
        self.pump, self.deadline = pump, deadline
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            self.socket.settimeout(0.1)
            self.socket.connect(path)
            key = base64.b64encode(os.urandom(16)).decode("ascii")
            self.socket.sendall((f"GET /content/{CHAIN} HTTP/1.1\r\nHost: localhost\r\n"
                                 "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                 f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
            header = bytearray()
            while not header.endswith(b"\r\n\r\n"):
                if len(header) >= 8192:
                    raise Failure("oversized WebSocket handshake")
                header.extend(self.exact(1))
            lines = header.decode("ascii").split("\r\n")
            fields = {}
            for line in lines[1:]:
                if line:
                    name, separator, value = line.partition(":")
                    if not separator or name.lower() in fields:
                        raise Failure("invalid WebSocket handshake headers")
                    fields[name.lower()] = value.strip()
            expected = base64.b64encode(hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
            if (not lines[0].startswith("HTTP/1.1 101 ")
                    or fields.get("sec-websocket-accept") != expected
                    or fields.get("upgrade", "").lower() != "websocket"
                    or "upgrade" not in fields.get("connection", "").lower().split(", ")):
                raise Failure("WebSocket upgrade was not authenticated")
        except BaseException:
            self.socket.close()
            raise

    def exact(self, size):
        data = bytearray()
        while len(data) < size:
            self.pump(0)
            if time.monotonic() >= self.deadline:
                raise Failure("WebSocket read deadline exceeded")
            try:
                chunk = self.socket.recv(size - len(data))
            except socket.timeout:
                continue
            if not chunk:
                raise Failure("WebSocket disconnected before required evidence")
            data.extend(chunk)
        return bytes(data)

    def send(self, value):
        payload = json.dumps(value, separators=(",", ":")).encode()
        mask = os.urandom(4)
        if len(payload) >= 126:
            header = b"\x81\xfe" + struct.pack("!H", len(payload))
        else:
            header = bytes((0x81, 0x80 | len(payload)))
        self.socket.sendall(header + mask + bytes(byte ^ mask[i % 4]
                                                  for i, byte in enumerate(payload)))

    def frame(self):
        first, second = self.exact(2)
        opcode, size = first & 15, second & 127
        if first & 0x70 or not first & 0x80 or second & 0x80:
            raise Failure("unsupported fragmented, masked or RSV WebSocket frame")
        if size == 126:
            size = struct.unpack("!H", self.exact(2))[0]
        elif size == 127:
            size = struct.unpack("!Q", self.exact(8))[0]
        if opcode not in (1, 2, 8) or size > (65536 if opcode == 2 else 4096):
            raise Failure("unexpected or oversized content frame")
        return opcode, self.exact(size)

    def partial(self, fixture):
        self.send({"version": 1, "cid": fixture.cid, "max_bytes": fixture.bytes})
        opcode, raw = self.frame()
        expected = {"event": "started", "version": 1, "cid": fixture.cid, "chain": CHAIN}
        if opcode != 1 or json.loads(raw) != expected:
            raise Failure("content stream did not start with matching identity")
        opcode, raw = self.frame()
        if opcode != 2 or not 0 < len(raw) < fixture.bytes:
            raise Failure("adversarial connection did not receive real partial bytes")
        return len(raw)

    def close(self):
        self.socket.close()


@dataclass
class Child:
    role: str
    process: subprocess.Popen
    stdout: bytearray = field(default_factory=bytearray)
    streams: int = 2


class ContentSoak:
    def __init__(self, args):
        self.args = args
        self.started = time.monotonic()
        self.ready_at = self.end = None
        self.selector = selectors.DefaultSelector()
        self.children = []
        self.logs = {}
        self.daemon = None
        self.stopping = False
        self.previous = None
        self.next_sample = 0
        self.start_ticks = None
        self.clock_ticks = os.sysconf("SC_CLK_TCK")
        self.metrics = {name: soak.Metrics() for name in ("all", "steady")}
        self.pss = {name: {"samples": 0, "sum_bytes": 0, "peak_bytes": 0,
                           "first_bytes": None, "last_bytes": None} for name in self.metrics}
        self.counts = Counter({name: 0 for name in (
            "downloads_started", "downloads_verified", "pairs_verified", "cancelled_partial",
            "writer_timeouts_observed", "recovery_pairs", "failures", "timeouts", "read_errors")})
        self.failure = None
        self.phase = "setup"
        self.sequence = 0
        self.active_peer = None
        self.daemon_timeouts = 0

    def record(self, kind, **fields):
        self.logs["events"].append(json.dumps({"event": kind, "elapsed_secs": time.monotonic() - self.started,
                                               **fields}, sort_keys=True).encode())

    def prepare(self, path):
        self.socket = path
        for role in ("daemon", "probe", "fetch", "events", "samples"):
            self.logs[role] = soak.TailLog(self.args.output_dir / (role + ".log"))
        self.binaries = {"daemon": self.args.bin_dir / "smolhostd",
                         "probe": self.args.bin_dir / "smolhost",
                         "fetch": self.args.bin_dir / "examples" / "fetch-blob"}
        for executable in self.binaries.values():
            if not executable.is_file() or not os.access(executable, os.X_OK):
                raise Failure("already-built executable unavailable: " + str(executable))
        specs = [ROOT / "chains" / name for name in SPECS]
        metadata = {"schema_version": 1, "started_unix_secs": time.time(), "socket": path,
                    "platform": list(os.uname()), "python": sys.version,
                    "scripts": [soak.identity(Path(__file__).resolve()), soak.identity(Path(soak.__file__))],
                    "binaries": {name: soak.identity(value) for name, value in self.binaries.items()},
                    "specs": [soak.identity(spec) for spec in specs],
                    "options": {key: str(value) if isinstance(value, Path) else value
                                for key, value in vars(self.args).items()},
                    "cpu_units": "one fully utilized core = 100%; may exceed 100%",
                    "bounds": {"content_connections": 2, "log_files": 10,
                               "log_file_bytes": soak.LOG_BYTES, "download_files": 2},
                    "privacy": "Local paths and peer logs: keep raw evidence private"}
        with (self.args.output_dir / "metadata.json").open("x") as file:
            soak.emit(file, metadata)
        command = [str(self.binaries["daemon"]), "--socket", path, "--no-state",
                   "--chain-spec", str(specs[0])]
        for spec in specs[1:]:
            command.extend(("--parachain-spec", str(spec)))
        self.daemon = self.spawn("daemon", command)

    def spawn(self, role, command):
        environment = os.environ.copy()
        environment.update(RUST_LOG="info", RUST_LOG_STYLE="never")
        for key in ("LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES", "NOTIFY_SOCKET"):
            environment.pop(key, None)
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=environment, start_new_session=True)
        child = Child(role, process)
        self.children.append(child)  # Own the child before any fallible evidence I/O.
        for channel in ("stdout", "stderr"):
            stream = getattr(process, channel)
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, (child, channel, bytearray()))
        self.record("spawn", role=role, pid=process.pid, command=command)
        return child

    def sample(self):
        now = time.monotonic()
        if now < self.next_sample or self.daemon is None or self.stopping:
            return
        sample = soak.read_proc(self.daemon.process.pid, self.start_ticks, pss=True)
        self.start_ticks = sample["starttime_ticks"]
        sample.update(elapsed_secs=now - self.started, cpu_percent=None,
                      cpu_interval_seconds=0, interval_secs=0)
        steady_at = None if self.ready_at is None else self.ready_at + self.args.warmup_secs
        sample["steady"] = steady_at is not None and now >= steady_at
        if self.previous is not None:
            previous_time, previous_ticks = self.previous
            delta = (sample["cpu_ticks"] - previous_ticks) / self.clock_ticks
            interval = now - previous_time
            if delta < 0 or interval <= 0:
                raise Failure("invalid daemon CPU sampling interval")
            sample.update(cpu_percent=100 * delta / interval,
                          cpu_interval_seconds=delta, interval_secs=interval)
        for name in ("all", "steady"):
            if name == "steady" and not sample["steady"]:
                continue
            include_cpu = self.previous is not None and (name == "all" or self.previous[0] >= steady_at)
            self.metrics[name].add(sample, include_cpu)
            if sample["pss_bytes"] is not None:
                metric = self.pss[name]
                metric["samples"] += 1
                metric["sum_bytes"] += sample["pss_bytes"]
                metric["peak_bytes"] = max(metric["peak_bytes"], sample["pss_bytes"])
                if metric["first_bytes"] is None:
                    metric["first_bytes"] = sample["pss_bytes"]
                metric["last_bytes"] = sample["pss_bytes"]
        self.logs["samples"].append(json.dumps(sample, sort_keys=True).encode())
        self.previous = now, sample["cpu_ticks"]
        self.next_sample = now + self.args.sample_secs

    def line(self, child, raw):
        self.logs[child.role].append(raw)
        if child.role == "daemon":
            if b"host: session ended: content write timed out" in raw:
                self.daemon_timeouts += 1
            if not self.stopping and (b"panicked at" in raw or b" ERROR " in raw):
                raise Failure("daemon error; see private daemon.log")

    def pump(self, timeout=0.1):
        for key, _ in self.selector.select(timeout):
            child, channel, buffer = key.data
            try:
                raw = os.read(key.fd, 65536)
            except BlockingIOError:
                continue
            if not raw:
                if buffer:
                    self.line(child, bytes(buffer))
                self.selector.unregister(key.fileobj)
                key.fileobj.close()
                child.streams -= 1
                continue
            if child.role != "daemon" and channel == "stdout":
                child.stdout.extend(raw)
                if len(child.stdout) > 65536:
                    raise Failure("child stdout exceeds bounded receipt/probe evidence limit")
            buffer.extend(raw)
            while b"\n" in buffer:
                line, _, rest = buffer.partition(b"\n")
                buffer[:] = rest
                self.line(child, bytes(line))
            if len(buffer) > soak.MAX_LINE:
                raise Failure("child log line exceeds bounded evidence limit")
        if not self.stopping and self.daemon is not None and self.daemon.process.poll() is not None:
            raise Failure("private daemon exited unexpectedly")
        self.sample()

    def pause(self, seconds):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.pump(min(0.1, deadline - time.monotonic()))

    def wait_children(self, children, deadline):
        while any(child.process.poll() is None or child.streams for child in children):
            if time.monotonic() >= deadline:
                raise Failure("child operation deadline exceeded")
            self.pump()
        for child in children:
            code = child.process.wait()
            if code != 0:
                raise Failure(f"{child.role} failed with exit {code}; see private log")
            self.children.remove(child)

    def ready(self):
        self.phase = "readiness"
        deadline = time.monotonic() + 300
        while not Path(self.socket).exists():
            if time.monotonic() >= deadline:
                raise Failure("private socket readiness deadline exceeded")
            self.pump()
        child = self.spawn("probe", [str(self.binaries["probe"]), "--socket", self.socket,
                                    "--chain", CHAIN, "probe", "--finalizations", "1",
                                    "--timeout-secs", str(max(1, int(deadline - time.monotonic())))])
        self.wait_children([child], deadline)
        evidence = soak.Evidence(GENESIS, 1)
        for line in child.stdout.splitlines():
            evidence.accept(json.loads(line))
        if evidence.phase != "done":
            raise Failure("Bulletin finalized probe lacks validated completion")
        self.ready_at = time.monotonic()
        self.end = self.ready_at + self.args.duration_secs
        self.record("ready", genesis=GENESIS, finalized_block=evidence.last_block)

    def pair(self, fixture, recovery=False):
        self.phase = "recovery" if recovery else "download"
        deadline = min(self.end, time.monotonic() + 180)
        children = []
        for slot in range(2):
            self.sequence += 1
            output = self.args.output_dir / f"download-{self.sequence}.bin"
            child = self.spawn("fetch", [str(self.binaries["fetch"]), "--socket", self.socket,
                                        "--chain", CHAIN, "--cid", fixture.cid,
                                        "--max-bytes", str(fixture.bytes), "--timeout-secs", "175",
                                        "--output", str(output)])
            self.counts["downloads_started"] += 1
            children.append((child, output))
        self.wait_children([child for child, _ in children], deadline)
        for child, output in children:
            receipt = verify_download(output, child.stdout, fixture)
            self.counts["downloads_verified"] += 1
            self.record("verified", pid=child.process.pid, recovery=recovery, **receipt)
        self.counts["pairs_verified"] += 1
        if recovery:
            self.counts["recovery_pairs"] += 1
        # Successful clients must not leave their own hidden partial files behind.
        if any(self.args.output_dir.glob(".smolhost-*")):
            raise Failure("successful download left partial files")

    def adversary(self, fixture, slow):
        self.phase = "slow_reader" if slow else "cancellation"
        before = self.daemon_timeouts
        self.active_peer = WebSocket(self.socket, self.pump, min(self.end, time.monotonic() + 175))
        try:
            partial = self.active_peer.partial(fixture)
            started = time.monotonic()
            self.record("partial_received", scenario=self.phase, bytes=partial)
            if slow:
                # No reads/polls on this socket. The daemon may need three 5s
                # deadlines (data, error, close) before logging its original error.
                deadline = min(self.end, started + 25)
                while self.daemon_timeouts == before:
                    if time.monotonic() >= deadline:
                        raise Failure("nonreader did not produce a live content writer timeout")
                    self.pump()
                if self.daemon_timeouts != before + 1 or time.monotonic() - started < 4:
                    raise Failure("writer timeout evidence is ambiguous or premature")
                self.counts["writer_timeouts_observed"] += 1
                self.record("writer_timeout", partial_bytes=partial,
                            nonreading_secs=time.monotonic() - started,
                            proof="private daemon content write timed out log; sole content session")
            else:
                self.active_peer.send({"event": "cancel"})
                self.active_peer.deadline = min(self.end, time.monotonic() + 10)
                # Require peer closure, not merely that our own close succeeded.
                opcode, _ = self.active_peer.frame()
                drained = partial
                while opcode == 2:
                    drained += len(_)
                    if drained >= fixture.bytes:
                        raise Failure("cancellation raced with a complete file")
                    opcode, _ = self.active_peer.frame()
                if opcode != 8:
                    raise Failure("cancellation did not close the partial stream")
                self.counts["cancelled_partial"] += 1
                self.record("cancelled", partial_bytes=drained)
        finally:
            self.active_peer.close()
            self.active_peer = None

    def run(self):
        root = Fixture(self.args.cid, self.args.expected_sha256,
                       self.args.expected_bytes, self.args.expected_blocks)
        churn = root if self.args.churn_cid is None else Fixture(
            self.args.churn_cid, self.args.churn_sha256, self.args.churn_bytes, self.args.churn_blocks)
        self.ready()
        self.pair(root)
        self.adversary(root, slow=False)
        self.pair(root, recovery=True)
        self.adversary(root, slow=True)
        self.pair(root, recovery=True)
        cycle = 0
        while time.monotonic() < self.end:
            self.phase = "steady"
            self.pause(min(self.args.interval_secs, max(0, self.end - time.monotonic())))
            if self.end - time.monotonic() < 180:
                continue
            cycle += 1
            self.pair(root if cycle % self.args.full_every == 0 else churn)
        if self.daemon_timeouts != self.counts["writer_timeouts_observed"]:
            raise Failure("unexpected content writer timeout outside adversarial session")
        if self.metrics["steady"].count < 2:
            raise Failure("insufficient warmup-separated steady resource samples")
        self.phase = "completed"

    def fail(self, error):
        self.counts["failures"] += 1
        text = type(error).__name__ + ": " + str(error)
        if "timeout" in text.lower() or "deadline" in text.lower() or "timed out" in text.lower():
            self.counts["timeouts"] += 1
        if isinstance(error, OSError) or "read" in text.lower() or "disconnected" in text.lower():
            self.counts["read_errors"] += 1
        if self.failure is None:
            self.failure = {"phase": self.phase, "reason": text}

    def cleanup(self):
        self.stopping = True

        def attempt(operation):
            try:
                return operation()
            except Exception as error:
                self.fail(error)
                return None

        if self.active_peer is not None:
            attempt(self.active_peer.close)
            self.active_peer = None
        # Terminate only our direct children, never by name, service, or socket.
        # Even failed evidence I/O or one child's cleanup cannot strand the rest.
        for child in self.children:
            if child.process.poll() is None:
                try:
                    child.process.terminate()
                except ProcessLookupError:
                    pass
                except OSError as error:
                    self.fail(error)
        deadline = time.monotonic() + 15
        try:
            while any(child.process.poll() is None or child.streams for child in self.children):
                if time.monotonic() >= deadline:
                    break
                self.pump()
        except Exception as error:
            self.fail(error)
        finally:
            for child in self.children:
                if child.process.poll() is None:
                    try:
                        child.process.kill()
                        self.counts["shutdown_kills"] += 1
                    except ProcessLookupError:
                        pass
                    except OSError as error:
                        self.fail(error)
                code = attempt(lambda: child.process.wait(timeout=5))
                if child is self.daemon and code != 0:
                    self.fail(Failure(f"private daemon shutdown exit was {code}"))
                for stream in (child.process.stdout, child.process.stderr):
                    attempt(stream.close)
            attempt(self.selector.close)
            for log in self.logs.values():
                attempt(log.close)

    def summary(self):
        resources = {}
        for name, metric in self.metrics.items():
            pss = self.pss[name]
            resources[name] = {**metric.summary(), "pss_samples": pss["samples"],
                               "pss_mean_bytes": pss["sum_bytes"] / pss["samples"] if pss["samples"] else None,
                               "pss_peak_bytes": pss["peak_bytes"] if pss["samples"] else None,
                               "pss_first_bytes": pss["first_bytes"], "pss_last_bytes": pss["last_bytes"]}
        return {"ok": self.failure is None and self.phase == "completed", "failure": self.failure,
                "phase": self.phase, "counts": dict(self.counts), "resources": resources,
                "elapsed_secs": time.monotonic() - self.started,
                "workload_secs": None if self.ready_at is None else time.monotonic() - self.ready_at,
                "output_dir": str(self.args.output_dir),
                "boundary": "Public live retrieval only; not proof of chain inclusion or memory ceiling"}


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cid", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--expected-bytes", type=soak.positive_integer, required=True)
    parser.add_argument("--expected-blocks", type=soak.positive_integer, required=True)
    parser.add_argument("--duration-secs", type=soak.positive_integer, default=900)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--warmup-secs", type=soak.nonnegative_integer, default=120)
    parser.add_argument("--sample-secs", type=soak.positive_integer, default=5)
    parser.add_argument("--interval-secs", type=soak.positive_integer, default=30)
    parser.add_argument("--full-every", type=soak.positive_integer, default=8)
    parser.add_argument("--churn-cid", help="Optional smaller public fixture; all four churn pins required")
    parser.add_argument("--churn-sha256")
    parser.add_argument("--churn-bytes", type=soak.positive_integer)
    parser.add_argument("--churn-blocks", type=soak.positive_integer)
    args = parser.parse_args(argv)
    pins = (args.churn_cid, args.churn_sha256, args.churn_bytes, args.churn_blocks)
    if any(pin is not None for pin in pins) and not all(pin is not None for pin in pins):
        parser.error("all four --churn-* pins are required together")
    for cid, digest, size, blocks in ((args.cid, args.expected_sha256, args.expected_bytes,
                                      args.expected_blocks), *((pins,) if args.churn_cid else ())):
        if not re.fullmatch(r"b[a-z2-7]+", cid) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            parser.error("fixture needs canonical lowercase base32 CID and lowercase SHA256")
        if size > MAX_BYTES or blocks > 4096:
            parser.error("fixture exceeds content limits")
    if args.expected_blocks < 2 or args.expected_bytes < 2 * 1024 * 1024:
        parser.error("primary fixture must be chunked and >=2 MiB to force live writer backpressure")
    if args.duration_secs < max(240, args.warmup_secs + 2 * args.sample_secs):
        parser.error("duration must be >=240s and leave at least two steady samples after warmup")
    args.bin_dir = args.bin_dir.resolve()
    args.output_dir = args.output_dir.absolute()
    # Raw evidence must not enter source control or disposable target outputs.
    resolved = args.output_dir.resolve()
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
    if resolved.is_relative_to(ROOT) or resolved.is_relative_to(target) or resolved.is_relative_to(args.bin_dir):
        parser.error("output directory must be outside the repository, target and binary directory")
    return args


def main(argv=None):
    args = arguments(argv)
    os.umask(0o077)
    try:
        args.output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    except OSError as error:
        print(json.dumps({"ok": False, "reason": "cannot reserve new evidence directory: " + str(error)}))
        return 1
    harness = ContentSoak(args)
    temporary = None

    def interrupted(signum, _frame):
        raise KeyboardInterrupt("signal " + str(signum))

    previous = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        temporary = tempfile.TemporaryDirectory(prefix="sh-c-", dir="/tmp")
        os.chmod(temporary.name, 0o700)
        harness.prepare(str(Path(temporary.name) / "rpc.sock"))
        harness.run()
    except (Exception, KeyboardInterrupt) as error:
        harness.fail(error)
    finally:
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
        try:
            harness.cleanup()
        except Exception as error:
            harness.fail(error)
        finally:
            try:
                if temporary is not None:
                    temporary.cleanup()
            except Exception as error:
                harness.fail(error)
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    summary = harness.summary()
    try:
        with (args.output_dir / "summary.json").open("x") as file:
            soak.emit(file, summary)
            os.fsync(file.fileno())
    except OSError as error:
        summary.update(ok=False, summary_error=str(error))
    print(json.dumps(summary, sort_keys=True, allow_nan=False), flush=True)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

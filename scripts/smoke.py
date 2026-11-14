#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Opt-in live two-process acceptance check; uses already-built Rust binaries."""

import argparse
from collections import Counter, deque
from dataclasses import dataclass, field
import errno
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import tempfile
import time


HASH = re.compile(r"0x[0-9a-fA-F]{64}\Z")
VALUE = re.compile(r"0x[0-9a-fA-F]{8}\Z")
CHAIN_ID = re.compile(r"[A-Za-z0-9_-]+\Z")
INITIALIZATION = re.compile(r"(Chain|Parachain) initialization complete for ([^ ]+)\. Name: ")
MAX_LINE = 1024 * 1024
MAX_EVENTS = 4096


class SmokeFailure(Exception):
    pass


@dataclass
class Child:
    name: str
    process: subprocess.Popen
    events: list = field(default_factory=list)
    stderr: deque = field(default_factory=lambda: deque(maxlen=64))
    stderr_lines: int = 0
    stdout_eof: bool = False


class Smoke:
    def __init__(self, args):
        self.args = args
        self.deadline = time.monotonic() + args.timeout_secs
        self.selector = selectors.DefaultSelector()
        self.children = {}
        self.buffers = {}
        self.ready = False
        self.engine_ready = False
        self.relay_chain = None
        self.parachains = []
        self.expected_initializations = Counter()
        self.initializations = Counter()
        self.session_counts = deque(maxlen=128)
        self.logs = deque(maxlen=128)
        self.a_exited = False
        self.genesis = None
        self.sequence = 0

    def configure_chains(self):
        ids = []
        for path in [self.args.chain_spec, *self.args.parachain_spec]:
            if not path.is_file():
                raise SmokeFailure("chain specification is not a regular file")
            try:
                specification = json.loads(path.read_text(encoding="utf-8"))
            except (ValueError, UnicodeError, RecursionError):
                raise SmokeFailure("chain specification is not valid JSON") from None
            chain = specification.get("id") if isinstance(specification, dict) else None
            if not isinstance(chain, str) or not CHAIN_ID.fullmatch(chain):
                raise SmokeFailure("chain specification has an invalid route ID")
            if chain in ids:
                raise SmokeFailure("configured chain specifications have duplicate route IDs")
            ids.append(chain)
        self.relay_chain, *self.parachains = ids
        self.expected_initializations = Counter(
            [("Chain", self.relay_chain)]
            + [("Parachain", chain) for chain in self.parachains]
        )
        if self.args.chain not in ids:
            raise SmokeFailure("requested route is not a configured chain")

    def start(self, name, command):
        environment = os.environ.copy()
        # Keep the required host and smoldot info logs visible and reproducible.
        environment["RUST_LOG"] = "info"
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            start_new_session=True,
        )
        child = Child(name, process)
        self.children[name] = child
        for channel in ("stdout", "stderr"):
            stream = getattr(process, channel)
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, (child, channel))
            self.buffers[stream.fileno()] = bytearray()

    def daemon_line(self, text):
        if "host: listening socket=" in text:
            self.ready = True
            self.logs.append("host: listening socket=<temporary socket>")
        match = re.search(r"host: engine ready chain=([A-Za-z0-9_-]+)(?:\s|$)", text)
        if match:
            if match[1] not in [self.relay_chain, *self.parachains]:
                raise SmokeFailure("daemon initialized an unexpected chain")
            if match[1] == self.relay_chain:
                self.engine_ready = True
            self.logs.append("host: engine ready chain=" + match[1])
        match = re.search(r"host: session (opened|closed) active=(\d+)(?:\s|$)", text)
        if match:
            active = int(match[2])
            if active > 2:
                raise SmokeFailure("unexpected extra daemon session")
            self.session_counts.append(active)
            self.logs.append(f"host: session {match[1]} active={active}")
        for match in INITIALIZATION.finditer(text):
            key = (match[1], match[2])
            self.initializations[key] += 1
            if key not in self.expected_initializations:
                raise SmokeFailure("smoldot initialized an unexpected chain or chain kind")
            if self.initializations[key] > 1:
                raise SmokeFailure("smoldot initialized a configured chain more than once")
            self.logs.append(f"smoldot: {match[1].lower()} initialization complete chain={match[2]}")

    def client_line(self, child, text):
        try:
            event = json.loads(text)
        except (ValueError, RecursionError):
            raise SmokeFailure(child.name + " emitted invalid JSONL") from None
        if not isinstance(event, dict):
            raise SmokeFailure(child.name + " emitted a non-object event")
        kind = event.get("event")
        if kind not in ("initialized", "storage", "finalized", "completed"):
            raise SmokeFailure(child.name + " emitted an unexpected event")
        genesis = event.get("genesis_hash")
        block = event.get("block_hash")
        if not isinstance(genesis, str) or not HASH.fullmatch(genesis):
            raise SmokeFailure(child.name + " emitted an invalid genesis hash")
        if not isinstance(block, str) or not HASH.fullmatch(block):
            raise SmokeFailure(child.name + " emitted an invalid block hash")
        genesis = genesis.lower()
        if self.genesis is None:
            self.genesis = genesis
        elif self.genesis != genesis:
            raise SmokeFailure("clients disagree on the genesis hash")
        if kind != "initialized":
            number = event.get("number")
            if type(number) is not int or not 0 <= number <= 0xFFFFFFFF:
                raise SmokeFailure(child.name + " did not return numeric block storage")
        if kind == "storage":
            value = event.get("value")
            if not isinstance(value, str) or not VALUE.fullmatch(value):
                raise SmokeFailure(child.name + " returned null or invalid block storage")
            if int.from_bytes(bytes.fromhex(value[2:]), "little") != event["number"]:
                raise SmokeFailure(child.name + " returned inconsistent decoded storage")
        previous = [record["event"] for record in child.events]
        if not previous and kind != "initialized":
            raise SmokeFailure(child.name + " emitted data before initialization")
        if "completed" in previous or (kind == "initialized" and previous):
            raise SmokeFailure(child.name + " emitted an invalid event sequence")
        if kind in ("finalized", "completed"):
            count = sum(item == "finalized" for item in previous)
            expected = count + 1 if kind == "finalized" else count
            if type(event.get("finalizations")) is not int or event["finalizations"] != expected:
                raise SmokeFailure(child.name + " emitted inconsistent finalization counts")
            if not any(
                item["event"] == "storage"
                and item["block_hash"] == block.lower()
                and item["number"] == event["number"]
                for item in child.events
            ):
                raise SmokeFailure(child.name + " finalized without matching storage")
        if len(child.events) >= MAX_EVENTS:
            raise SmokeFailure(child.name + " exceeded the event limit")
        # Store only the public, validated proof fields, not arbitrary JSON payloads.
        record = {
            "event": kind,
            "block_hash": block.lower(),
            "after_a_exit": self.a_exited,
            "sequence": self.sequence,
        }
        for key in ("number", "finalizations"):
            if key in event:
                record[key] = event[key]
        child.events.append(record)

    def line(self, child, channel, raw):
        self.sequence += 1
        text = raw.decode("utf-8", errors="replace")
        if channel == "stderr":
            child.stderr_lines += 1
            # Raw diagnostics are transient, bounded, and never printed or saved.
            child.stderr.append(text[:4096])
            if child.name == "daemon":
                self.daemon_line(text)
        elif child.name != "daemon":
            self.client_line(child, text)

    def pump(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise SmokeFailure("overall live acceptance deadline exceeded")
        # The short timeout only reaps exits; readiness is exclusively log-driven.
        for key, _ in self.selector.select(min(remaining, 0.1)):
            child, channel = key.data
            stream = key.fileobj
            descriptor = stream.fileno()
            try:
                chunk = os.read(descriptor, 65536)
            except BlockingIOError:
                continue
            buffer = self.buffers[descriptor]
            buffer.extend(chunk)
            while b"\n" in buffer:
                raw, _, rest = buffer.partition(b"\n")
                buffer[:] = rest
                if len(raw) > MAX_LINE:
                    raise SmokeFailure(child.name + " exceeded the log line limit")
                self.line(child, channel, raw)
            if len(buffer) > MAX_LINE:
                raise SmokeFailure(child.name + " exceeded the log line limit")
            if not chunk:
                if buffer:
                    self.line(child, channel, bytes(buffer))
                del self.buffers[descriptor]
                self.selector.unregister(stream)
                stream.close()
                if channel == "stdout":
                    child.stdout_eof = True
        daemon = self.children["daemon"]
        if daemon.process.poll() is not None:
            raise SmokeFailure("daemon exited before acceptance finished")
        for name in ("A", "B"):
            child = self.children.get(name)
            if child is None:
                continue
            status = child.process.poll()
            if status is not None and status != 0:
                raise SmokeFailure("client " + name + " exited unsuccessfully")
            if status == 0 and child.stdout_eof:
                required = 1 if name == "A" else 4
                counts = Counter(item["event"] for item in child.events)
                if counts["initialized"] != 1 or counts["completed"] != 1:
                    raise SmokeFailure("client " + name + " exited without completing its probe")
                if counts["finalized"] != required or not counts["storage"]:
                    raise SmokeFailure("client " + name + " returned incomplete finalization/storage evidence")
                if name == "A":
                    self.a_exited = True

    def post_disconnect_blocks(self):
        child = self.children.get("B")
        if child is None:
            return []
        storage = {
            item["block_hash"]: item
            for item in child.events
            if item["event"] == "storage" and item["after_a_exit"]
        }
        return [
            {"block_hash": item["block_hash"], "number": item["number"]}
            for item in child.events
            if item["event"] == "finalized"
            and item["after_a_exit"]
            and item["block_hash"] in storage
            and storage[item["block_hash"]]["sequence"] < item["sequence"]
        ]

    def run(self, socket):
        command = [str(self.args.bin_dir / "polkadot-hostd"),
                   "--chain-spec", str(self.args.chain_spec), "--socket", socket, "--no-state"]
        for path in self.args.parachain_spec:
            command.extend(["--parachain-spec", str(path)])
        self.start("daemon", command)
        while not self.ready:
            self.pump()
        for name, finalizations in (("A", 1), ("B", 4)):
            self.start(name, [str(self.args.bin_dir / "polkadot-host"),
                              "--socket", socket, "--chain", self.args.chain,
                              "probe", "--finalizations", str(finalizations),
                              "--timeout-secs", str(self.args.timeout_secs)])
        while True:
            self.pump()
            finished = all(self.children[name].process.poll() == 0
                           and self.children[name].stdout_eof for name in ("A", "B"))
            if finished:
                if not self.post_disconnect_blocks():
                    raise SmokeFailure("B did not produce finalized storage after A exited")
                if 2 not in self.session_counts:
                    raise SmokeFailure("daemon never observed two concurrent sessions")
                if not self.engine_ready or self.initializations != self.expected_initializations:
                    raise SmokeFailure("missing exactly one smoldot initialization per configured chain")
                # The closed-session log can precede OS process reaping. Require
                # zero active handles once both successful exits are observed.
                if self.session_counts and self.session_counts[-1] == 0:
                    return

    def cleanup(self):
        for child in self.children.values():
            if child.process.poll() is None:
                child.process.terminate()
        grace = time.monotonic() + 5
        for child in self.children.values():
            try:
                child.process.wait(timeout=max(0, grace - time.monotonic()))
            except subprocess.TimeoutExpired:
                pass
        for child in self.children.values():
            if child.process.poll() is None:
                child.process.kill()
            child.process.wait()
        self.selector.close()
        for child in self.children.values():
            for channel in ("stdout", "stderr"):
                stream = getattr(child.process, channel)
                if stream is not None:
                    stream.close()

    def report(self, failure):
        clients = {}
        for name in ("A", "B"):
            child = self.children.get(name)
            if child is not None:
                storage = [item for item in child.events if item["event"] == "storage"]
                clients[name] = {
                    "exit_code": child.process.returncode,
                    "events": dict(Counter(item["event"] for item in child.events)),
                    "last_storage": ({"block_hash": storage[-1]["block_hash"],
                                      "number": storage[-1]["number"]} if storage else None),
                    "stderr_lines": child.stderr_lines,
                }
        report = {
            "ok": failure is None,
            "chain": self.args.chain,
            "relay_chain": self.relay_chain,
            "parachains": self.parachains,
            "genesis_hash": self.genesis,
            "clients": clients,
            "b_finalized_storage_after_a_exit": self.post_disconnect_blocks(),
            "concurrent_sessions_observed": 2 in self.session_counts,
            "session_cleanup_observed": bool(self.session_counts) and self.session_counts[-1] == 0,
            "smoldot_chain_initializations": sum(
                count for (kind, _), count in self.initializations.items() if kind == "Chain"
            ),
            "smoldot_parachain_initializations": {
                chain: self.initializations[("Parachain", chain)] for chain in self.parachains
            },
            "configured_initializations_observed": (
                bool(self.expected_initializations)
                and self.initializations == self.expected_initializations
            ),
            "sharing_evidence": (
                "one smoldot initialization per configured chain; two concurrent RPC session handles"
                if self.expected_initializations
                and self.initializations == self.expected_initializations
                and 2 in self.session_counts else "not established"
            ),
            "sharing_evidence_limit": "session counts alone do not prove shared network synchronization",
        }
        if failure is not None:
            report["error"] = failure
            report["diagnostics"] = {
                child.name: [self.sanitize(line) for line in list(child.stderr)[-8:]]
                for child in self.children.values() if child.stderr
            }
        for line in self.logs:
            print(line, file=sys.stderr)
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))

    def sanitize(self, text):
        for path in (str(self.args.chain_spec), *(str(path) for path in self.args.parachain_spec),
                     str(self.args.bin_dir), str(Path.home())):
            text = text.replace(path, "<local-path>")
        return re.sub(r"/tmp/polkadot-host-smoke-[^/\s]+", "<temporary-directory>", text)


def positive_integer(value):
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive integer") from None
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, default=Path("target/debug"))
    parser.add_argument("--chain-spec", type=Path, required=True)
    parser.add_argument("--parachain-spec", type=Path, action="append", default=[],
                        help="parachain specification to load alongside the relay (repeatable)")
    parser.add_argument("--chain", required=True)
    parser.add_argument("--timeout-secs", type=positive_integer, default=300)
    args = parser.parse_args()
    if not CHAIN_ID.fullmatch(args.chain):
        parser.error("chain must contain only ASCII letters, digits, underscores, or hyphens")
    smoke = Smoke(args)
    failure = None
    temporary = None

    def interrupted(_signal, _frame):
        raise KeyboardInterrupt

    previous_terminate = signal.signal(signal.SIGTERM, interrupted)
    try:
        for binary in ("polkadot-hostd", "polkadot-host"):
            path = args.bin_dir / binary
            if not path.is_file() or not os.access(path, os.X_OK):
                raise SmokeFailure("required already-built executable unavailable: " + binary)
        smoke.configure_chains()
        # A fixed system temporary root avoids excessively long Unix socket paths.
        temporary = tempfile.TemporaryDirectory(prefix="polkadot-host-smoke-", dir="/tmp")
        os.chmod(temporary.name, 0o700)
        smoke.run(str(Path(temporary.name) / "rpc.sock"))
    except SmokeFailure as error:
        failure = str(error)
    except KeyboardInterrupt:
        failure = "interrupted"
    except OSError as error:
        failure = "operating system error: " + errno.errorcode.get(error.errno, "UNKNOWN")
    except Exception as error:
        failure = "unexpected driver error: " + type(error).__name__ + ": " + smoke.sanitize(str(error))
    finally:
        # A second interrupt must not strand children partway through cleanup.
        previous_interrupt = signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        smoke.cleanup()
        if temporary is not None:
            temporary.cleanup()
        signal.signal(signal.SIGINT, previous_interrupt)
        signal.signal(signal.SIGTERM, previous_terminate)
    smoke.report(failure)
    return 0 if failure is None else 1


if __name__ == "__main__":
    sys.exit(main())

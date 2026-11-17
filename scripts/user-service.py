#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Install an opt-in user service; preserve conflicting or modified files."""

import argparse
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import stat
import subprocess
import sys

REPO = Path(__file__).resolve().parent.parent
UNITS = ("smolhost.service", "smolhost.socket")
CHAINS = ("paseo.json", "paseo_next_asset_hub.json", "paseo_next_bulletin.json",
          "paseo_next_individuality.json")
FILES = ("smolhost", "smolhostd", *CHAINS, *UNITS)
RECEIPT = "installed-files.json"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def exists(path):
    return os.path.lexists(path)


def systemctl(*arguments):
    subprocess.run(["systemctl", "--user", *arguments], check=True, timeout=30)


def layout():
    home = Path.home()
    config = Path(os.environ.get("XDG_CONFIG_HOME") or home / ".config")
    if not home.is_absolute() or not config.is_absolute():
        raise RuntimeError("HOME and XDG_CONFIG_HOME must be absolute")
    bundle = home / ".local/lib/smolhost"
    links = {home / ".local/bin/smolhost": bundle / "smolhost"}
    files = {
        name: config / "systemd/user" / name if name in UNITS else bundle / name
        for name in FILES
    }
    return bundle, files, links


def package_sources(bin_dir):
    sources = {
        "smolhost": bin_dir / "smolhost",
        "smolhostd": bin_dir / "smolhostd",
        **{name: REPO / "chains" / name for name in CHAINS},
        **{name: REPO / "systemd" / name for name in UNITS},
    }
    for name, source in sources.items():
        if source.is_symlink() or not source.is_file():
            raise RuntimeError(f"missing or symlinked package file: {source}")
        if name in ("smolhost", "smolhostd") and not os.access(source, os.X_OK):
            raise RuntimeError(f"binary is not executable: {source}")
    return sources


def install(bin_dir):
    bundle, files, links = layout()
    for destination in (bundle, *files.values(), *links):
        if exists(destination):
            raise RuntimeError(f"refusing to replace {destination}; uninstall the old installation first")
    states = subprocess.check_output(
        ["systemctl", "--user", "show", *UNITS, "--property=LoadState", "--value"], text=True
    ).split()
    if states != ["not-found", "not-found"]:
        raise RuntimeError("a smolhost unit is already known to the user manager; refusing to shadow it")
    sources = package_sources(bin_dir)
    bundle.parent.mkdir(parents=True, exist_ok=True)
    bundle.mkdir(mode=0o700)
    created = []
    linked = []
    try:
        checksums = {}
        for name, source in sources.items():
            destination = files[name]
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as output:
                created.append(destination)
                with source.open("rb") as stream:
                    shutil.copyfileobj(stream, output)
            destination.chmod(0o755 if name in ("smolhost", "smolhostd") else 0o644)
            checksums[name] = digest(destination)
        receipt = bundle / RECEIPT
        with receipt.open("x") as output:
            created.append(receipt)
            json.dump(checksums, output, sort_keys=True)
            output.write("\n")
        for link, destination in links.items():
            link.parent.mkdir(parents=True, exist_ok=True)
            link.symlink_to(destination)
            linked.append(link)
        systemctl("daemon-reload")
    except BaseException:
        for link in reversed(linked):
            if link.is_symlink() and link.readlink() == links[link]:
                link.unlink()
        for path in reversed(created):
            path.unlink(missing_ok=True)
        bundle.rmdir()
        raise
    print("Installed; nothing has been started or enabled.")
    print("Start at login and now: systemctl --user enable --now smolhost.service")
    print("Or start only for this session: systemctl --user start smolhost.service")


def owned_installation(allow_missing=False):
    bundle, files, links = layout()
    if bundle.is_symlink() or not bundle.is_dir():
        raise RuntimeError("no installer-owned bundle found")
    receipt = bundle / RECEIPT
    if receipt.is_symlink():
        raise RuntimeError("refusing a symlinked installation receipt")
    checksums = json.loads(receipt.read_text())
    required = {"smolhost", "smolhostd", *UNITS}
    if (
        not isinstance(checksums, dict)
        or not required.issubset(checksums)
        or not set(checksums).issubset(files)
    ):
        raise RuntimeError("invalid installation receipt")
    if any(not isinstance(value, str) or len(value) != 64
           or any(char not in "0123456789abcdef" for char in value)
           for value in checksums.values()):
        raise RuntimeError("invalid installation checksums")
    # Check everything before stopping services or deleting anything.
    for name in checksums:
        path = files[name]
        if not exists(path) and not allow_missing:
            raise RuntimeError(f"missing installation file: {path}")
        if exists(path) and (path.is_symlink() or not path.is_file() or digest(path) != checksums[name]):
            raise RuntimeError(f"modified installation file: {path}; preserve it before proceeding")
    for link, destination in links.items():
        if not exists(link) and not allow_missing:
            raise RuntimeError(f"missing installation link: {link}")
        if exists(link) and (not link.is_symlink() or link.readlink() != destination):
            raise RuntimeError(f"refusing to remove an unrelated file: {link}")
    return checksums


def uninstall():
    bundle, files, links = layout()
    checksums = owned_installation(allow_missing=True)
    receipt = bundle / RECEIPT
    systemctl("disable", "--now", *UNITS)
    for link in links:
        if exists(link):
            if not link.is_symlink() or link.readlink() != links[link]:
                raise RuntimeError(f"installation changed during uninstall: {link}")
            link.unlink()
    # The receipt, not today's package contents, defines what this install owns.
    for name in checksums:
        files[name].unlink(missing_ok=True)
    receipt.unlink()
    try:
        bundle.rmdir()
    except OSError as error:
        if error.errno != errno.ENOTEMPTY:
            raise
        print(f"Preserved additional files in {bundle}")
    systemctl("daemon-reload")
    print("Uninstalled. No chain state or unrelated files were removed.")


def transaction_path():
    return layout()[0].parent / ".smolhost-upgrade"


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fingerprint(path):
    if not exists(path):
        return None
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o7000:
        raise RuntimeError(f"not an ordinary package file: {path}")
    return {"sha256": digest(path), "mode": stat.S_IMODE(info.st_mode)}


def save_journal(directory, record):
    temporary = directory / "journal.new"
    with temporary.open("w") as output:
        json.dump(record, output, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(directory / "journal.json")
    sync_directory(directory)


def copy_verified(source, destination, expected):
    checksum = hashlib.sha256()
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise RuntimeError(f"not a regular source file: {source}")
        with destination.open("xb") as output:
            while chunk := stream.read(1024 * 1024):
                output.write(chunk)
                checksum.update(chunk)
            if checksum.hexdigest() != expected["sha256"]:
                raise RuntimeError(f"source changed during upgrade: {source}")
            os.fchmod(output.fileno(), expected["mode"])
            output.flush()
            os.fsync(output.fileno())


def unit_states(check_paths=False, allow_transition=False):
    _, files, _ = layout()
    result = {}
    for unit in UNITS:
        text = subprocess.check_output(
            ["systemctl", "--user", "show", unit,
             "--property=ActiveState,UnitFileState,FragmentPath,DropInPaths"],
            text=True, timeout=30,
        )
        properties = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
        active = properties.get("ActiveState")
        enabled = properties.get("UnitFileState")
        allowed = {"active", "inactive", "failed"}
        if allow_transition:
            allowed.update(("activating", "deactivating", "reloading"))
        if active not in allowed or not enabled:
            raise RuntimeError(f"{unit} is changing state or unavailable; retry when settled")
        if check_paths and (properties.get("FragmentPath") != str(files[unit])
                            or properties.get("DropInPaths")):
            raise RuntimeError(f"{unit} has a different source or drop-ins; preserve custom configuration manually")
        result[unit] = {"active": active not in ("inactive", "failed"), "enabled": enabled}
    return result


def stop_units():
    result = subprocess.run(["systemctl", "--user", "stop", *UNITS], timeout=30)
    if any(item["active"] for item in unit_states().values()):
        raise RuntimeError("could not stop the service and socket; transaction retained")
    # An invalid replacement unit can make `stop` fail while both units are
    # already inactive. Recovery must still be able to restore the valid files.
    if result.returncode:
        print("Stop reported an error; both units are confirmed inactive.", file=sys.stderr)


def health_check():
    bundle, _, _ = layout()
    result = subprocess.run(
        [str(bundle / "smolhost"), "--timeout-secs", "10", "chains"],
        text=True, capture_output=True, check=True, timeout=15,
    )
    info = json.loads(result.stdout)
    expected = {json.loads((bundle / name).read_text())["id"] for name in CHAINS}
    if (not isinstance(info, dict) or info.get("protocol_version") != 1
            or info.get("read_only") is not True
            or {chain["id"] for chain in info.get("chains", [])} != expected):
        raise RuntimeError("replacement daemon did not expose the installed chain catalog")


def restore_units(states):
    # Socket-only installations briefly activate for the health check, then
    # return to socket-only operation. Never change enablement or linger policy.
    if any(state["active"] for state in states.values()):
        # Candidate failures must not exhaust the original service's restart
        # budget and prevent rollback from bringing the old binary back.
        systemctl("reset-failed", *UNITS)
    for unit in reversed(UNITS):
        if states[unit]["active"]:
            systemctl("start", unit)
    if any(state["active"] for state in states.values()):
        health_check()
        if not states[UNITS[0]]["active"]:
            systemctl("stop", UNITS[0])
    current = unit_states()
    for unit in UNITS:
        if current[unit]["enabled"] != states[unit]["enabled"]:
            raise RuntimeError("unit enablement changed during upgrade")
        # A new application may legitimately reactivate a listening socket.
        if unit == UNITS[0] and states[UNITS[1]]["active"] and not states[unit]["active"]:
            continue
        if current[unit]["active"] != states[unit]["active"]:
            raise RuntimeError("service/socket activity was not restored")


def targets():
    bundle, files, _ = layout()
    return {**files, RECEIPT: bundle / RECEIPT}


def temporary_path(destination, record):
    return destination.with_name(f".{destination.name}.smolhost-{record['token']}.tmp")


def remove_temporary(path):
    if exists(path):
        if path.is_symlink() or not path.is_file() or path.stat().st_uid != os.geteuid():
            raise RuntimeError(f"refusing unexpected transaction temporary: {path}")
        path.unlink()


def replace_file(directory, record, name, side):
    destination = targets()[name]
    expected = record["files"][name][side]
    temporary = temporary_path(destination, record)
    remove_temporary(temporary)
    if expected is None:
        destination.unlink(missing_ok=True)
    else:
        copy_verified(directory / side / name, temporary, expected)
        temporary.replace(destination)
    sync_directory(destination.parent)


def cleanup_transaction(directory, record):
    # Remove only this journal's named artifacts, never arbitrary bundle files.
    if {path.name for path in directory.iterdir()} - {"journal.json", "journal.new", "before", "after"}:
        raise RuntimeError("preserving unexpected files in the upgrade transaction")
    for side in ("before", "after"):
        folder = directory / side
        if folder.exists() and {path.name for path in folder.iterdir()} - set(record["files"]):
            raise RuntimeError(f"preserving unexpected files in {folder}")
    for name in record["files"]:
        remove_temporary(temporary_path(targets()[name], record))
        for side in ("before", "after"):
            remove_temporary(directory / side / name)
    for side in ("before", "after"):
        if (directory / side).exists():
            (directory / side).rmdir()
    (directory / "journal.new").unlink(missing_ok=True)
    (directory / "journal.json").unlink()
    directory.rmdir()
    sync_directory(directory.parent)


def load_journal(directory):
    if directory.is_symlink() or not directory.is_dir():
        raise RuntimeError("invalid upgrade transaction directory")
    info = directory.stat()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise RuntimeError("upgrade transaction must be private and owned by this user")
    journal = directory / "journal.json"
    if journal.is_symlink():
        raise RuntimeError("refusing symlinked upgrade journal")
    record = json.loads(journal.read_text())
    if not isinstance(record, dict) or record.get("schema") != 1:
        raise RuntimeError("invalid upgrade journal")
    token = record.get("token")
    if (not isinstance(token, str) or len(token) != 32
            or any(char not in "0123456789abcdef" for char in token)):
        raise RuntimeError("invalid upgrade transaction token")
    if record.get("phase") not in ("preparing", "prepared", "committed", "rolled-back"):
        raise RuntimeError("invalid upgrade phase")
    entries = record.get("files")
    if not isinstance(entries, dict) or set(entries) != set(targets()):
        raise RuntimeError("invalid upgrade file set")
    for versions in entries.values():
        if not isinstance(versions, dict) or set(versions) != {"before", "after"}:
            raise RuntimeError("invalid upgrade file record")
        for side, value in versions.items():
            if value is None and side == "before":
                continue
            if (not isinstance(value, dict) or set(value) != {"sha256", "mode"}
                    or not isinstance(value["sha256"], str) or len(value["sha256"]) != 64
                    or any(char not in "0123456789abcdef" for char in value["sha256"])
                    or type(value["mode"]) is not int or not 0 <= value["mode"] <= 0o777):
                raise RuntimeError("invalid upgrade fingerprint")
    states = record.get("states")
    if not isinstance(states, dict) or set(states) != set(UNITS):
        raise RuntimeError("invalid saved unit state")
    for state in states.values():
        if (not isinstance(state, dict) or set(state) != {"active", "enabled"}
                or type(state["active"]) is not bool or not isinstance(state["enabled"], str)
                or not state["enabled"]):
            raise RuntimeError("invalid saved unit state")
    for side in ("before", "after"):
        folder = directory / side
        if folder.is_symlink() or (folder.exists() and not folder.is_dir()):
            raise RuntimeError("invalid upgrade backup directory")
    return record


def recover():
    directory = transaction_path()
    if not exists(directory):
        print("No interrupted upgrade to recover.")
        return
    if directory.is_dir() and not directory.is_symlink() and not exists(directory / "journal.json"):
        # Interrupted first journal write or final directory removal: no package
        # replacement is possible here. Refuse anything beyond that owned write.
        if {path.name for path in directory.iterdir()} <= {"journal.new"}:
            owned_installation()
            remove_temporary(directory / "journal.new")
            directory.rmdir()
            sync_directory(directory.parent)
            print("Removed an interrupted journal preparation/cleanup.")
            return
    record = load_journal(directory)
    _, _, links = layout()
    for link, destination in links.items():
        if not link.is_symlink() or link.readlink() != destination:
            raise RuntimeError(f"installation link changed; recovery retained: {link}")
    for name, versions in record["files"].items():
        actual = fingerprint(targets()[name])
        if record["phase"] in ("committed", "rolled-back"):
            side = "after" if record["phase"] == "committed" else "before"
            allowed = [versions[side]]
        else:
            allowed = list(versions.values())
        if actual not in allowed:
            raise RuntimeError(f"installation changed outside upgrade; recovery retained: {name}")
        if record["phase"] == "preparing" and actual != versions["before"]:
            raise RuntimeError(f"installation changed during preparation: {name}")
    if record["phase"] == "prepared":
        # Validate ALL backups and enablement before stopping or replacing anything.
        for name, versions in record["files"].items():
            if versions["before"] is not None:
                if fingerprint(directory / "before" / name) != versions["before"]:
                    raise RuntimeError(f"invalid upgrade backup: {name}; recovery retained")
        current = unit_states(allow_transition=True)
        if any(current[unit]["enabled"] != record["states"][unit]["enabled"] for unit in UNITS):
            raise RuntimeError("unit enablement changed outside upgrade; recovery retained")
        stop_units()
        for name in record["files"]:
            replace_file(directory, record, name, "before")
        systemctl("daemon-reload")
        restore_units(record["states"])
        record["phase"] = "rolled-back"
        save_journal(directory, record)
    cleanup_transaction(directory, record)
    print("Recovered the interrupted upgrade; committed upgrades are not rolled back.")


def upgrade(bin_dir):
    bundle, files, _ = layout()
    checksums = owned_installation()
    sources = package_sources(bin_dir)
    states = unit_states(check_paths=True)
    for name, path in files.items():
        if name not in checksums and exists(path):
            raise RuntimeError(f"new package file conflicts with an unrelated file: {path}")
    before = {name: fingerprint(path) for name, path in targets().items()}
    after = {name: fingerprint(source) for name, source in sources.items()}
    for name, value in after.items():
        value["mode"] = 0o755 if name in ("smolhost", "smolhostd") else 0o644
    receipt = (json.dumps({name: value["sha256"] for name, value in after.items()},
                          sort_keys=True) + "\n").encode()
    after[RECEIPT] = {"sha256": hashlib.sha256(receipt).hexdigest(), "mode": 0o600}
    record = {
        "schema": 1, "phase": "preparing", "token": secrets.token_hex(16), "states": states,
        "files": {name: {"before": before[name], "after": after[name]} for name in targets()},
    }
    directory = transaction_path()
    directory.mkdir(mode=0o700)
    save_journal(directory, record)
    sync_directory(directory.parent)
    try:
        for side in ("before", "after"):
            (directory / side).mkdir(mode=0o700)
        for name, versions in record["files"].items():
            if versions["before"] is not None:
                copy_verified(targets()[name], directory / "before" / name, versions["before"])
            if name != RECEIPT:
                copy_verified(sources[name], directory / "after" / name, versions["after"])
        with (directory / "after" / RECEIPT).open("xb") as output:
            output.write(receipt)
            os.fchmod(output.fileno(), 0o600)
            output.flush()
            os.fsync(output.fileno())
        for side in ("before", "after"):
            sync_directory(directory / side)
        # A second preflight detects edits made while large binaries were staged.
        if any(fingerprint(path) != before[name] for name, path in targets().items()):
            raise RuntimeError("installation changed while staging the upgrade")
        if unit_states(check_paths=True) != states:
            raise RuntimeError("unit state changed while staging the upgrade")
        record["phase"] = "prepared"
        save_journal(directory, record)
        stop_units()
        for name in record["files"]:
            replace_file(directory, record, name, "after")
        systemctl("daemon-reload")
        restore_units(states)
        record["phase"] = "committed"
        save_journal(directory, record)
    except BaseException as error:
        try:
            recover()
        except BaseException as recovery_error:
            raise RuntimeError(f"upgrade failed ({error}); recovery retained ({recovery_error}); "
                               "inspect the problem, then run recover") from error
        raise RuntimeError(f"upgrade failed and was rolled back: {error}") from error
    cleanup_transaction(directory, record)
    print("Upgraded; prior service/socket activity and enablement preserved. Chain state was not moved.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("install", "upgrade", "recover", "uninstall"))
    parser.add_argument("--bin-dir", type=Path, default=REPO / "target/debug")
    args = parser.parse_args()
    if os.geteuid() == 0:
        parser.error("run as your regular user, not with sudo")
    try:
        bundle, _, _ = layout()
        bundle.parent.mkdir(parents=True, exist_ok=True)
        lock_path = bundle.parent / ".smolhost-install.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(fd, "r+b") as lock:
            if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
                raise RuntimeError("installer lock is not a regular file")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError("another installer operation is running") from error
            if args.action != "recover" and exists(transaction_path()):
                raise RuntimeError("an upgrade journal exists; run recover before another operation")
            if args.action == "install":
                install(args.bin_dir)
            elif args.action == "upgrade":
                upgrade(args.bin_dir)
            elif args.action == "recover":
                recover()
            else:
                uninstall()
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

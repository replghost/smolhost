#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Install an opt-in user service; preserve conflicting or modified files."""

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
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
    subprocess.run(["systemctl", "--user", *arguments], check=True)


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
    sources = {
        "smolhost": bin_dir / "smolhost",
        "smolhostd": bin_dir / "smolhostd",
        **{name: REPO / "chains" / name for name in CHAINS},
        **{name: REPO / "systemd" / name for name in UNITS},
    }
    for name, source in sources.items():
        if not source.is_file():
            raise RuntimeError(f"missing {source}; build both binaries first")
        if name in ("smolhost", "smolhostd") and not os.access(source, os.X_OK):
            raise RuntimeError(f"binary is not executable: {source}")
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


def uninstall():
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
    # Check everything before stopping services or deleting anything.
    for name in checksums:
        path = files[name]
        if exists(path) and (path.is_symlink() or not path.is_file() or digest(path) != checksums[name]):
            raise RuntimeError(f"modified installation file: {path}; preserve it before uninstalling")
    for link, destination in links.items():
        if exists(link) and (not link.is_symlink() or link.readlink() != destination):
            raise RuntimeError(f"refusing to remove an unrelated file: {link}")
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("install", "uninstall"))
    parser.add_argument("--bin-dir", type=Path, default=REPO / "target/debug")
    args = parser.parse_args()
    if os.geteuid() == 0:
        parser.error("run as your regular user, not with sudo")
    try:
        if args.action == "install":
            install(args.bin_dir)
        else:
            uninstall()
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

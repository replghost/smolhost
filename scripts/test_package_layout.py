#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Offline package-layout safety tests; real binaries/managers are qualified separately."""

import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import tarfile
import tempfile
import unittest

import package_layout as layout

REPO = Path(__file__).resolve().parent.parent


def make_bundle(directory, version="0.1.0-alpha.1", floor="2.34"):
    directory.mkdir(parents=True)
    payload = {name: f"fixture: {name}\n".encode() for name in layout.REQUIRED}
    for name in ("smolhost.service", "smolhost.socket"):
        payload[f"systemd/{name}"] = (REPO / "systemd" / name).read_bytes()
    payload["third-party/dependencies.json"] = b'{"packages": []}\n'
    payload["third-party/licenses/fixture/LICENSE"] = b"upstream license notice\n"
    files = {}
    for name, data in payload.items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        mode = 0o755 if name.startswith("bin/") else 0o644
        path.chmod(mode)
        files[name] = {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data), "mode": f"{mode:04o}"}
    manifest = {"schema_version": 1, "name": "smolhost", "kind": "binary", "target": layout.TARGET,
                "version": version, "glibc_floor": floor, "files": files}
    (directory / layout.MANIFEST).write_text(json.dumps(manifest) + "\n")
    (directory / layout.MANIFEST).chmod(0o644)
    return manifest


def make_archive(directory, version="0.1.0-alpha.1", floor="2.34"):
    """Shared isolated fixture for native builders; bytes are not executable applications."""
    directory.mkdir(parents=True, exist_ok=True)
    bundle = directory / "bundle"
    make_bundle(bundle, version, floor)
    archive = directory / f"smolhost-{version}.tar.gz"
    with tarfile.open(archive, "w:gz") as output:
        for path in sorted(bundle.rglob("*")):
            if path.is_file():
                output.add(path, arcname=path.relative_to(bundle).as_posix(), recursive=False)
    return archive, layout.sha256(archive)


class LayoutTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / "bundle"
        self.manifest = make_bundle(self.bundle)
        self.destination = self.root / "package"

    def test_package_files_are_usable_by_normal_users_under_private_build_umask(self):
        old = os.umask(0o077)
        try:
            layout.stage_bundle(self.bundle, self.destination)
        finally:
            os.umask(old)
        for path in (self.destination, *self.destination.rglob("*")):
            relative = path.relative_to(self.destination).as_posix()
            if path.is_dir():
                self.assertEqual(path.stat().st_mode & 0o777, 0o755, relative)
            elif relative.startswith("usr/bin/"):
                self.assertEqual(path.stat().st_mode & 0o777, 0o755, relative)
            else:
                self.assertEqual(path.stat().st_mode & 0o777, 0o644, relative)
        for name in ("smolhost", "smolhostd"):
            self.assertEqual((self.destination / "usr/bin" / name).read_bytes(),
                             (self.bundle / "bin" / name).read_bytes())
        self.assertEqual((self.destination / "usr/share/licenses/smolhost/third-party/licenses/fixture/LICENSE").read_bytes(),
                         b"upstream license notice\n")
        unit = (self.destination / "usr/lib/systemd/user/smolhost.service").read_text()
        command = shlex.split(next(line.split("=", 1)[1] for line in unit.splitlines() if line.startswith("ExecStart=")))
        self.assertEqual(command[0], "/usr/bin/smolhostd")
        for index, argument in enumerate(command):
            if argument in ("--chain-spec", "--parachain-spec"):
                source = command[index + 1]
                self.assertTrue(source.startswith("/usr/share/smolhost/chains/"))
                self.assertTrue((self.destination / source.lstrip("/")).is_file())
        self.assertFalse((self.destination / "etc").exists())
        self.assertFalse((self.destination / "home").exists())

    def test_makepkg_source_extras_are_not_installed_or_followed(self):
        private = self.root / "private"
        private.write_text("private unrelated file")
        (self.bundle / "source-cache.tar.gz").symlink_to(private)
        (self.bundle / "package_layout.py").write_text("source helper, not a runtime dependency")
        layout.stage_bundle(self.bundle, self.destination)
        for path in self.destination.rglob("*"):
            if path.is_file():
                self.assertNotIn(b"private unrelated file", path.read_bytes())
                self.assertNotIn(b"source helper, not a runtime dependency", path.read_bytes())

    def test_changed_binary_refused_before_staging(self):
        (self.bundle / "bin/smolhostd").write_text("not the pinned binary")
        with self.assertRaises(ValueError):
            layout.stage_bundle(self.bundle, self.destination)
        self.assertFalse(self.destination.exists())

    def test_symlinked_payload_directory_refused_without_copying_private_files(self):
        real = self.bundle / "bin"
        real.rename(self.root / "outside")
        real.symlink_to(self.root / "outside", target_is_directory=True)
        with self.assertRaises(ValueError):
            layout.stage_bundle(self.bundle, self.destination)
        self.assertFalse(self.destination.exists())

    def test_existing_destination_is_preserved(self):
        self.destination.mkdir()
        sentinel = self.destination / "keep"
        sentinel.write_bytes(b"user-owned")
        with self.assertRaises(ValueError):
            layout.stage_bundle(self.bundle, self.destination)
        self.assertEqual(sentinel.read_bytes(), b"user-owned")
        self.assertEqual(list(self.destination.iterdir()), [sentinel])

    def test_new_unmapped_user_path_refused_before_staging(self):
        path = self.bundle / "systemd/smolhost.service"
        path.write_text(path.read_text() + '\nEnvironmentFile="%h/.local/lib/smolhost/new.conf"\n')
        entry = self.manifest["files"]["systemd/smolhost.service"]
        entry.update(size=path.stat().st_size, sha256=layout.sha256(path))
        (self.bundle / layout.MANIFEST).write_text(json.dumps(self.manifest))
        with self.assertRaises(ValueError):
            layout.stage_bundle(self.bundle, self.destination)
        self.assertFalse(self.destination.exists())


class ExtractionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.archive, self.checksum = make_archive(self.root / "input")
        self.destination = self.root / "extracted"

    def test_exact_pinned_release_extracts_and_stages_without_changing_binaries(self):
        layout.extract_bundle(self.archive, self.destination, self.checksum)
        staging = self.root / "package"
        layout.stage_bundle(self.destination, staging)
        self.assertEqual((staging / "usr/bin/smolhostd").read_bytes(),
                         (self.root / "input/bundle/bin/smolhostd").read_bytes())

    def test_bad_archive_pin_cannot_create_a_destination(self):
        with self.assertRaises(ValueError):
            layout.extract_bundle(self.archive, self.destination, "0" * 64)
        self.assertFalse(self.destination.exists())

    def test_escape_symlink_and_duplicate_members_are_rejected(self):
        for kind in ("escape", "symlink", "duplicate"):
            with self.subTest(kind=kind):
                bad = self.root / f"{kind}.tar.gz"
                with tarfile.open(bad, "w:gz") as output:
                    member = tarfile.TarInfo("../outside" if kind == "escape" else "bin/smolhost")
                    member.mode = 0o644
                    if kind == "symlink":
                        member.type = tarfile.SYMTYPE
                        member.linkname = "/etc/passwd"
                        output.addfile(member)
                    else:
                        member.size = 1
                        output.addfile(member, io.BytesIO(b"x"))
                        if kind == "duplicate":
                            output.addfile(member, io.BytesIO(b"x"))
                with self.assertRaises(ValueError):
                    layout.extract_bundle(bad, self.destination, layout.sha256(bad))
                self.assertFalse(self.destination.exists())
                self.assertFalse((self.root / "outside").exists())

    def test_wrong_target_cannot_become_a_native_package(self):
        manifest = json.loads((self.root / "input/bundle" / layout.MANIFEST).read_text())
        manifest["target"] = "aarch64-unknown-linux-gnu"
        with self.assertRaises(ValueError):
            layout.parse_manifest(json.dumps(manifest))


if __name__ == "__main__":
    unittest.main()

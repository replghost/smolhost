#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Isolated packaging tests; no application builds, service control or network.

Run with Python 3.11+. Tests that inspect real debs/version ordering require
Ubuntu's dpkg-deb/dpkg and otherwise skip. All payload fixtures are inert bytes.
"""

import argparse
import hashlib
import importlib.util
import io
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location("smolhost_build_deb", SCRIPTS / "build-deb.py")
deb = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deb)
VERSION = "0.1.0-alpha.1"


def digest(path, algorithm="sha256"):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, algorithm).hexdigest()


class VersionTests(unittest.TestCase):
    def test_manifest_version_mapping(self):
        for upstream, revision, expected in (
            (VERSION, 1, "0.1.0~alpha.1-1"),
            (VERSION, 2, "0.1.0~alpha.1-2"),
            ("0.1.0", 1, "0.1.0-1"),
            ("1.2.3-rc.10+build.5", 12, "1.2.3~rc.10+build.5-12"),
        ):
            with self.subTest(upstream=upstream, revision=revision):
                self.assertEqual(deb.debian_version(upstream, revision), expected)

    def test_invalid_versions_and_revisions(self):
        for version in ("", "v0.1.0", "0.1", "01.1.0", "0.1.0-01", "0.1.0-alpha..1",
                        "0.1.0\nArchitecture: all", "0.1.0/../../bad", None):
            with self.subTest(version=version), self.assertRaises(ValueError):
                deb.debian_version(version)
        for revision in (0, -1, "1", 1.5, True):
            with self.subTest(revision=revision), self.assertRaises(ValueError):
                deb.debian_version(VERSION, revision)
        for revision in ("0", "-1", "01", "1.0", "x", " 1", "+1"):
            with self.subTest(cli_revision=revision), self.assertRaises(argparse.ArgumentTypeError):
                deb.positive_revision(revision)
        self.assertEqual(deb.positive_revision("2"), 2)

    @unittest.skipUnless(shutil.which("dpkg"), "dpkg is required for Debian ordering")
    def test_actual_package_manager_upgrade_ordering(self):
        versions = [("0.1.0-alpha.1", 1), ("0.1.0-alpha.1", 2),
                    ("0.1.0-alpha.2", 1), ("0.1.0-beta.9", 1),
                    ("0.1.0-beta.10", 1), ("0.1.0-rc.1", 1),
                    ("0.1.0", 1), ("0.1.0", 2), ("0.1.1", 1)]
        for earlier, later in zip(versions, versions[1:]):
            with self.subTest(earlier=earlier, later=later):
                subprocess.run(["dpkg", "--compare-versions", deb.debian_version(*earlier),
                                "lt", deb.debian_version(*later)], check=True)


class MetadataTests(unittest.TestCase):
    def test_runtime_dependencies_follow_manifest_floor(self):
        self.assertEqual(deb.glibc_dependency({"glibc_floor": "2.17"}), "2.34")
        self.assertEqual(deb.glibc_dependency({"glibc_floor": "2.34"}), "2.34")
        self.assertEqual(deb.glibc_dependency({"glibc_floor": "2.39"}), "2.39")
        for floor in (None, "", "2.34\nDepends: bad", "glibc-2.34"):
            with self.subTest(floor=floor), self.assertRaises(ValueError):
                deb.glibc_dependency({"glibc_floor": floor})

    def test_control_copyright_and_checksums_without_maintainer_hooks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "usr/share/doc/smolhost").mkdir(parents=True)
            (root / "usr/bin").mkdir()
            binary = root / "usr/bin/smolhost"
            binary.write_bytes(b"inert test payload\x00\xff")
            deb.write_metadata(root, {"version": VERSION, "glibc_floor": "2.34"},
                               deb.debian_version(VERSION))
            control_dir = root / "DEBIAN"
            self.assertEqual({path.name for path in control_dir.iterdir()}, {"control", "md5sums"})
            fields = (control_dir / "control").read_text()
            self.assertIn("Package: smolhost\n", fields)
            self.assertIn("Version: 0.1.0~alpha.1-1\n", fields)
            self.assertIn("Architecture: amd64\n", fields)
            self.assertIn("Maintainer: replghost <110247003+replghost@users.noreply.github.com>\n", fields)
            self.assertIn("Depends: libc6 (>= 2.34), libgcc-s1, systemd, dbus-user-session\n", fields)
            self.assertNotIn("python", fields.lower())
            self.assertNotIn("${", fields)
            copyright_path = root / "usr/share/doc/smolhost/copyright"
            copyright_text = copyright_path.read_text()
            self.assertIn(f"https://github.com/replghost/smolhost/releases/download/v{VERSION}/"
                          f"smolhost-v{VERSION}-source.tar.gz", copyright_text)
            self.assertIn("/usr/share/licenses/smolhost/third-party/licenses/", copyright_text)
            self.assertNotIn("${", copyright_text)
            for path in (control_dir / "control", control_dir / "md5sums", copyright_path):
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o644)
            self.assertIn(f"{digest(binary, 'md5')}  usr/bin/smolhost\n",
                          (control_dir / "md5sums").read_text())

    def test_invalid_checksum_and_revision_reject_without_creating_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            for checksum in ("", "a" * 63, "g" * 64, "a" * 65):
                with self.subTest(checksum=checksum), self.assertRaises(ValueError):
                    deb.build_deb(Path(temporary) / "missing", checksum, output)
                self.assertFalse(output.exists())
            with self.assertRaises(ValueError):
                deb.build_deb(Path(temporary) / "missing", "a" * 64, output, 0)
            self.assertFalse(output.exists())


class CommandErrorsTests(unittest.TestCase):
    def test_missing_dpkg_deb_has_actionable_error_and_no_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = subprocess.run(
                [sys.executable, str(SCRIPTS / "build-deb.py"), "--archive", str(root / "missing"),
                 "--sha256", "a" * 64, "--output", str(root / "output")],
                env={**os.environ, "PATH": str(root)}, capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("dpkg-deb is required", result.stderr)
            self.assertNotIn("Traceback", result.stderr)
            self.assertFalse((root / "output").exists())


@unittest.skipUnless(shutil.which("dpkg-deb"), "dpkg-deb is required for real deb tests")
class DebianPackageTests(unittest.TestCase):
    def setUp(self):
        from test_package_layout import make_archive
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        inputs = self.root / "input"
        inputs.mkdir()
        self.archive, self.sha256 = make_archive(inputs)
        self.output = self.root / "output"
        self.home = self.root / "home"
        self.home.mkdir()
        (self.home / "sentinel").write_text("untouched")

    def invoke(self, archive=None, checksum=None, output=None, revision=None):
        command = [sys.executable, str(SCRIPTS / "build-deb.py"),
                   "--archive", str(self.archive if archive is None else archive),
                   "--sha256", self.sha256 if checksum is None else checksum,
                   "--output", str(self.output if output is None else output)]
        if revision is not None:
            command.extend(["--revision", str(revision)])
        return subprocess.run(command, env={**os.environ, "HOME": str(self.home)},
                              capture_output=True, text=True)

    def built_package(self, **arguments):
        result = self.invoke(**arguments)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        output = arguments.get("output", self.output)
        artifacts = list(output.glob("*.deb"))
        self.assertEqual(len(artifacts), 1)
        self.assertEqual({path.name for path in output.iterdir()},
                         {artifacts[0].name, "SHA256SUMS"})
        return artifacts[0]

    def test_real_deb_metadata_ownership_payload_and_no_service_hooks(self):
        artifact = self.built_package(checksum=self.sha256.upper())
        self.assertEqual(artifact.name, "smolhost_0.1.0.alpha.1-1_amd64.deb")
        self.assertEqual((self.output / "SHA256SUMS").read_text(),
                         f"{digest(artifact)}  {artifact.name}\n")
        for option in ("--fsys-tarfile", "--ctrl-tarfile"):
            result = subprocess.run(["dpkg-deb", option, str(artifact)],
                                    capture_output=True, check=True)
            with tarfile.open(fileobj=io.BytesIO(result.stdout)) as package:
                members = package.getmembers()
                for member in members:
                    self.assertEqual((member.uid, member.gid), (0, 0), member.name)
                    self.assertTrue(member.isdir() or member.isfile(), member.name)
                    name = member.name.removeprefix("./").rstrip("/")
                    mode = 0o755 if member.isdir() or name in (
                        "usr/bin/smolhost", "usr/bin/smolhostd") else 0o644
                    self.assertEqual(member.mode, mode, member.name)
                    if name in ("", "."):
                        self.assertTrue(member.isdir(), "archive root must be a directory")
                        self.assertEqual(member.mode, 0o755)
                    elif option == "--fsys-tarfile":
                        self.assertTrue(name == "usr" or name.startswith("usr/"), name)
                contents = {member.name.removeprefix("./"): package.extractfile(member).read()
                            for member in members if member.isfile()}
            if option == "--ctrl-tarfile":
                self.assertEqual(set(contents), {"control", "md5sums"})
                self.assertIn(b"Architecture: amd64\n", contents["control"])
                self.assertIn(b"Version: 0.1.0~alpha.1-1\n", contents["control"])
                self.assertIn(b"Depends: libc6 (>= 2.34), libgcc-s1, systemd, dbus-user-session\n",
                              contents["control"])
                self.assertNotIn(b"python", contents["control"].lower())
                for line in contents["md5sums"].decode().splitlines():
                    expected, name = line.split("  ", 1)
                    self.assertEqual(hashlib.md5(payload[name]).hexdigest(), expected, name)
            else:
                payload = contents
                with tarfile.open(self.archive) as upstream:
                    for binary in ("smolhost", "smolhostd"):
                        self.assertEqual(contents[f"usr/bin/{binary}"],
                                         upstream.extractfile(f"bin/{binary}").read())
                    for member in upstream.getmembers():
                        if member.isfile() and member.name.startswith("third-party/"):
                            self.assertEqual(contents[f"usr/share/licenses/smolhost/{member.name}"],
                                             upstream.extractfile(member).read())
                    service = upstream.extractfile("systemd/smolhost.service").read().decode()
                    self.assertEqual(contents["usr/lib/systemd/user/smolhost.service"].decode(),
                                     deb.package_layout.packaged_service(service))
                    self.assertEqual(contents["usr/lib/systemd/user/smolhost.socket"],
                                     upstream.extractfile("systemd/smolhost.socket").read())
                    self.assertEqual(contents["usr/share/doc/smolhost/upstream-release-manifest.json"],
                                     upstream.extractfile("release-manifest.json").read())
                self.assertNotIn("usr/bin/user-service.py", contents)
                self.assertIn("usr/share/doc/smolhost/copyright", contents)
        self.assertEqual({path.name for path in self.home.iterdir()}, {"sentinel"})
        self.assertEqual((self.home / "sentinel").read_text(), "untouched")

    def test_revision_and_future_runtime_floor_reach_package_metadata(self):
        from test_package_layout import make_archive
        inputs = self.root / "new-input"
        inputs.mkdir()
        archive, checksum = make_archive(inputs, version="0.1.0", floor="2.39")
        artifact = self.built_package(archive=archive, checksum=checksum, revision=2)
        self.assertEqual(artifact.name, "smolhost_0.1.0-2_amd64.deb")
        result = subprocess.run(["dpkg-deb", "--field", str(artifact)], capture_output=True,
                                text=True, check=True)
        self.assertIn("Version: 0.1.0-2\n", result.stdout)
        self.assertIn("Depends: libc6 (>= 2.39),", result.stdout)

    def test_existing_outputs_and_symlinks_are_never_overwritten(self):
        self.output.mkdir()
        sentinel = self.output / "sentinel"
        sentinel.write_text("keep")
        regular = self.root / "regular"
        regular.write_text("keep")
        link = self.root / "link"
        link.symlink_to(self.root / "absent")
        for output in (self.output, regular, link):
            with self.subTest(output=output):
                result = self.invoke(output=output)
                self.assertEqual(result.returncode, 1)
                self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(sentinel.read_text(), "keep")
        self.assertEqual(regular.read_text(), "keep")
        self.assertTrue(link.is_symlink())
        self.assertFalse((self.root / "absent").exists())
        self.assertEqual({path.name for path in self.output.iterdir()}, {"sentinel"})

    def test_bad_checksum_missing_archive_and_malformed_archive_leave_no_output(self):
        broken = self.root / "broken.tar.gz"
        broken.write_bytes(b"not a release archive")
        for arguments in (
            {"checksum": "0" * 64},
            {"archive": self.root / "absent"},
            {"archive": broken, "checksum": digest(broken)},
        ):
            with self.subTest(arguments=arguments):
                result = self.invoke(**arguments)
                self.assertEqual(result.returncode, 1)
                self.assertNotIn("Traceback", result.stderr)
                self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()

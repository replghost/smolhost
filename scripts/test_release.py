#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Isolated release contract tests; Linux x86_64 GNU, Python 3.11+ and a C compiler.

Parser-only ELF structures are generated directly. Executable fixtures use the
platform C compiler/linker; no network, Cargo, real service manager or application
build is involved. These test packaging, not application behavior or Ubuntu
runtime qualification.
"""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest

SPEC = importlib.util.spec_from_file_location("smolhost_release", Path(__file__).with_name("release.py"))
release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = release
SPEC.loader.exec_module(release)
TEST_VERSION = "0.1.0-alpha.1"
PACKAGE_CHECKSUM = "a" * 64
SOURCE_TIME = 1_600_000_000_000_000_000
BINARY_TIME = SOURCE_TIME + 1_000_000_000


def elf_hash(text):
    result = 0
    for byte in text:
        result = (result << 4) + byte
        high = result & 0xf0000000
        if high:
            result ^= high >> 24
        result &= ~high
    return result


def elf_fixture(glibc="2.34", machine=62):
    """Structural ELF data for parser tests only; never execute this image."""
    base, size = 0x400000, 4096
    image = bytearray(size)
    image[:16] = b"\x7fELF\x02\x01\x01\x03" + b"\0" * 8
    struct.pack_into("<HHIQQQIHHHHHH", image, 16,
                     2, machine, 1, base + 0x300, 64, 0, 0, 64, 56, 4, 0, 0, 0)
    interpreter = b"/lib64/ld-linux-x86-64.so.2\0"
    version_name = f"GLIBC_{glibc}".encode("ascii")
    strings = b"\0libc.so.6\0" + version_name + b"\0"
    tags = [(1, 1), (5, base + 0x600), (10, len(strings)),
            (6, base + 0x700), (11, 24), (4, base + 0x720),
            (0x6ffffffe, base + 0x740), (0x6fffffff, 1), (0, 0)]
    headers = [(6, 4, 64, base + 64, 0, 56 * 4, 56 * 4, 8),
               (3, 4, 0x200, base + 0x200, 0, len(interpreter), len(interpreter), 1),
               (1, 5, 0, base, 0, size, size, 4096),
               (2, 4, 0x400, base + 0x400, 0, 16 * len(tags), 16 * len(tags), 8)]
    for index, header in enumerate(headers):
        struct.pack_into("<IIQQQQQQ", image, 64 + index * 56, *header)
    image[0x200:0x200 + len(interpreter)] = interpreter
    for index, pair in enumerate(tags):
        struct.pack_into("<qQ", image, 0x400 + index * 16, *pair)
    image[0x600:0x600 + len(strings)] = strings
    struct.pack_into("<IIII", image, 0x720, 1, 1, 0, 0)
    struct.pack_into("<HHIII", image, 0x740, 1, 1, 1, 16, 0)
    struct.pack_into("<IHHII", image, 0x750, elf_hash(version_name), 0, 2, 11, 0)
    return bytes(image)


class ElfTests(unittest.TestCase):
    def test_reports_dynamic_requirement_not_incidental_strings(self):
        data = elf_fixture(glibc="2.34") + b"GLIBC_9.99\0"
        result = release.elf_requirements(data)
        self.assertEqual(result["glibc_floor"], "2.34")
        self.assertIn("libc.so.6", result["needed_libraries"])
        self.assertEqual(release.elf_requirements(elf_fixture(glibc="2.39"))["glibc_floor"], "2.39")
        self.assertEqual(release.elf_requirements(elf_fixture(glibc="2.2.5"))["glibc_floor"], "2.2.5")

    def test_rejects_incompatible_architecture_or_abi(self):
        for data in (elf_fixture(machine=183), elf_fixture(glibc="2.40"),
                     elf_fixture(glibc="PRIVATE"), elf_fixture()[:70], b"#!/bin/sh\nexit 0\n"):
            with self.subTest(length=len(data)), self.assertRaises(release.ReleaseError):
                release.elf_requirements(data)

    def test_rejects_invalid_dynamic_chain_and_loader(self):
        for offset, fmt, value in ((0x200, "B", ord("X")), (0x74c, "I", 16),
                                   (0x75c, "I", 16), (0x408, "Q", 99999)):
            data = bytearray(elf_fixture())
            struct.pack_into(fmt, data, offset, value)
            with self.subTest(offset=offset), self.assertRaises(release.ReleaseError):
                release.elf_requirements(data)


class PathTests(unittest.TestCase):
    def test_rejects_unsafe_archive_names(self):
        for name in ("", "/etc/passwd", "../secret", "a/../../secret", "a//b", "a/./b",
                     "a/", "C:/secret", "a\\b", "a\nb", "a\0b"):
            with self.subTest(name=name), self.assertRaises(release.ReleaseError):
                release.archive_path(name)
        self.assertEqual(release.archive_path("vendor/crate-1.0.0/src/lib.rs"),
                         "vendor/crate-1.0.0/src/lib.rs")

    def test_rejects_file_directory_collision_before_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "archive.tar.gz"
            entry = release.Entry.generated(b"content")
            with self.assertRaises(release.ReleaseError):
                release.write_archive(destination, {"a": entry, "a/b": entry})
            self.assertFalse(destination.exists())


@unittest.skipUnless(sys.platform.startswith("linux") and platform.machine() == "x86_64" and
                     Path("/lib64/ld-linux-x86-64.so.2").exists(), "requires Linux x86_64 GNU")
class PackagingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Let the platform linker produce runnable inputs. A hand-built dynamic
        # image can pass structural parsing but crash inside the native loader.
        cls.native = {}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture"
            for version in (TEST_VERSION, "0.0.0"):
                program = (
                    '#include <stdio.h>\n#include <string.h>\n'
                    'int main(int argc, char **argv) { (void)argc;\n'
                    'const char *name = strrchr(argv[0], \'/\');\n'
                    f'printf("%s %s\\n", name ? name + 1 : argv[0], "{version}");\n'
                    'return 0;\n}\n'
                )
                subprocess.run(["cc", "-x", "c", "-", "-o", str(path)],
                               input=program, text=True, capture_output=True, check=True)
                cls.native[version] = path.read_bytes()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo, self.vendor, self.bins = (self.root / name for name in ("project", "vendor", "bins"))
        self.output = self.root / "artifacts"
        self.bins.mkdir()
        for name in release.SOURCE_FILES:
            path = self.repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("public fixture\n", encoding="utf-8")
        (self.repo / "Cargo.toml").write_text(
            f'[package]\nname = "smolhost"\nversion = "{TEST_VERSION}"\nedition = "2024"\n'
            '[dependencies]\nfixture = "=1.0.0"\n', encoding="utf-8")
        (self.repo / "Cargo.lock").write_text(
            'version = 4\n[[package]]\nname = "smolhost"\n'
            f'version = "{TEST_VERSION}"\ndependencies = ["fixture"]\n'
            '[[package]]\nname = "fixture"\nversion = "1.0.0"\n'
            f'source = "{release.REGISTRY}"\nchecksum = "{PACKAGE_CHECKSUM}"\n', encoding="utf-8")
        # Use the real installer and units, not a substitute with a different layout.
        for name in ("scripts/user-service.py", "systemd/smolhost.service", "systemd/smolhost.socket"):
            shutil.copyfile(release.REPO / name, self.repo / name)
        self.crate = self.vendor / "fixture-1.0.0"
        (self.crate / "src").mkdir(parents=True)
        (self.crate / "Cargo.toml").write_text(
            '[package]\nname = "fixture"\nversion = "1.0.0"\nlicense = "MIT OR Apache-2.0"\n'
            'build = "build.rs"\n[lib]\npath = "src/lib.rs"\n', encoding="utf-8")
        (self.crate / "src/lib.rs").write_text("pub fn fixture() {}\n", encoding="utf-8")
        (self.crate / "build.rs").write_text("fn main() {}\n", encoding="utf-8")
        (self.crate / "LICENSE-MIT").write_text("fixture license notice\n", encoding="utf-8")
        self.refresh_vendor_checksums()
        for name in release.SOURCE_FILES:
            os.utime(self.repo / name, ns=(SOURCE_TIME, SOURCE_TIME))
        for name in release.BINARIES:
            self.replace_binary(name)

    def refresh_vendor_checksums(self):
        files = {path.relative_to(self.crate).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                 for path in sorted(self.crate.rglob("*"))
                 if path.is_file() and path.name != ".cargo-checksum.json"}
        (self.crate / ".cargo-checksum.json").write_bytes(
            release.canonical_json({"package": PACKAGE_CHECKSUM, "files": files}))

    def replace_binary(self, name, version=TEST_VERSION, glibc=None):
        path = self.bins / name
        path.write_bytes(self.native[version] if glibc is None else elf_fixture(glibc=glibc))
        path.chmod(0o755)
        os.utime(path, ns=(BINARY_TIME, BINARY_TIME))

    def package(self, output=None):
        return release.package_release(self.repo, self.bins, self.vendor, output or self.output)

    def reject(self):
        with self.assertRaises((release.ReleaseError, OSError, ValueError)):
            self.package()
        self.assertFalse(os.path.lexists(self.output))

    def archive(self, kind):
        name = f"smolhost-v{TEST_VERSION}-{kind}.tar.gz"
        return tarfile.open(self.output / name, "r:gz")

    def test_complete_deterministic_archives_and_checksums(self):
        for name in (".env", ".git/config", "target/private", "evidence/private", "private.key"):
            path = self.repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("NEVER DISTRIBUTE THIS\n", encoding="utf-8")
        first_manifest = self.package()
        second = self.root / "artifacts-second"
        # Neither source mode, file timestamps nor locale-dependent ordering leaks into tar metadata.
        (self.repo / "README.md").chmod(0o600)
        os.utime(self.repo / "README.md", ns=(SOURCE_TIME + 42, SOURCE_TIME + 42))
        self.assertEqual(first_manifest, self.package(second))
        for path in self.output.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes(), path.name)
        sums = (self.output / "SHA256SUMS").read_text().splitlines()
        self.assertEqual(len(sums), 3)
        for line in sums:
            checksum, name = line.split("  ")
            self.assertEqual(checksum, hashlib.sha256((self.output / name).read_bytes()).hexdigest())
        local_manifest = json.loads((self.output / "artifact-manifest.json").read_bytes())
        self.assertEqual(local_manifest["glibc_floor"], "2.34")
        self.assertEqual(set(local_manifest["artifacts"]),
                         {f"smolhost-v{TEST_VERSION}-{kind}.tar.gz" for kind in ("linux-x86_64", "source")})
        for kind in ("linux-x86_64", "source"):
            with self.archive(kind) as archive:
                names = archive.getnames()
                self.assertEqual(names, sorted(names))
                self.assertEqual(len(names), len(set(names)))
                manifest = json.load(archive.extractfile("release-manifest.json"))
                self.assertEqual(set(manifest["files"]), set(names) - {"release-manifest.json"})
                self.assertEqual(manifest["source_snapshot"]["sha256"],
                                 local_manifest["source_snapshot_sha256"])
                for member in archive:
                    self.assertTrue(member.isfile())
                    self.assertEqual((member.uid, member.gid, member.mtime, member.uname, member.gname),
                                     (0, 0, 0, "", ""))
                    data = archive.extractfile(member).read()
                    self.assertNotIn(b"NEVER DISTRIBUTE THIS", data)
                    if member.name != "release-manifest.json":
                        metadata = manifest["files"][member.name]
                        self.assertEqual(metadata["sha256"], hashlib.sha256(data).hexdigest())
                        self.assertEqual(metadata["size"], len(data))
                        self.assertEqual(metadata["mode"], f"{member.mode:04o}")
                self.assertNotIn(str(self.root).encode(), archive.extractfile("release-manifest.json").read())
                notices = json.load(archive.extractfile("third-party/dependencies.json"))
                self.assertEqual(notices["packages"][0]["license_expression"], "MIT OR Apache-2.0")
                self.assertEqual(archive.extractfile(notices["packages"][0]["notice_files"][0]).read(),
                                 (self.crate / "LICENSE-MIT").read_bytes())
        with self.archive("source") as archive:
            for name in release.SOURCE_FILES:
                self.assertEqual(archive.extractfile(name).read(), (self.repo / name).read_bytes())
            self.assertEqual(archive.extractfile("vendor/fixture-1.0.0/build.rs").read(),
                             (self.crate / "build.rs").read_bytes())
            config = tomllib.loads(archive.extractfile(".cargo/config.toml").read().decode())
            self.assertEqual(config["source"]["vendored-sources"]["directory"], "vendor")
            self.assertEqual(config["source"]["crates-io"]["replace-with"], "vendored-sources")
            self.assertTrue(config["net"]["offline"])

    def test_binary_layout_can_be_consumed_by_real_installer(self):
        self.package()
        extracted = self.root / "extracted"
        extracted.mkdir()
        # Explicit writes avoid relying on extraction filters unavailable in older Python releases.
        with self.archive("linux-x86_64") as archive:
            for member in archive:
                path = extracted / release.archive_path(member.name)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(archive.extractfile(member).read())
                path.chmod(member.mode)
        spec = importlib.util.spec_from_file_location("release_installer", extracted / "scripts/user-service.py")
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        self.assertEqual(installer.REPO, extracted)
        for name in installer.CHAINS:
            self.assertTrue((installer.REPO / "chains" / name).is_file())
        for name in installer.UNITS:
            self.assertTrue((installer.REPO / "systemd" / name).is_file())
        for name in release.BINARIES:
            path = extracted / "bin" / name
            self.assertTrue(path.stat().st_mode & stat.S_IXUSR)
            result = subprocess.run([str(path), "--version"], check=True, capture_output=True, text=True)
            self.assertEqual(result.stdout.strip(), f"{name} {TEST_VERSION}")
        # Parser acceptance only: never contact a real or imitation systemd user manager.
        result = subprocess.run([sys.executable, "scripts/user-service.py", "install", "--bin-dir", "./bin", "--help"],
                                cwd=extracted, capture_output=True)
        self.assertEqual(result.returncode, 0)

    def test_rejects_binary_version_mismatch(self):
        self.replace_binary("smolhostd", version="0.0.0")
        self.reject()

    def test_rejects_binary_older_than_rust_input(self):
        os.utime(self.repo / "src/lib.rs", ns=(BINARY_TIME + 1, BINARY_TIME + 1))
        self.reject()

    def test_separate_daemon_rebuild_keeps_unchanged_cli_usable(self):
        daemon_source = self.repo / "src/bin/smolhostd.rs"
        daemon_source.write_text("changed daemon source\n")
        os.utime(daemon_source, ns=(BINARY_TIME + 1, BINARY_TIME + 1))
        os.utime(self.bins / "smolhostd", ns=(BINARY_TIME + 2, BINARY_TIME + 2))
        self.package()
        with self.archive("linux-x86_64") as archive:
            self.assertEqual(archive.extractfile("bin/smolhost").read(),
                             (self.bins / "smolhost").read_bytes())
        with self.archive("source") as archive:
            self.assertEqual(archive.extractfile("src/bin/smolhostd.rs").read(),
                             daemon_source.read_bytes())

    def test_rejects_incompatible_binary(self):
        self.replace_binary("smolhostd", glibc="2.40")
        self.reject()

    def test_rejects_non_executable_binary(self):
        (self.bins / "smolhostd").chmod(0o644)
        self.reject()

    def test_rejects_missing_source(self):
        (self.repo / "src/lib.rs").unlink()
        self.reject()

    def test_rejects_unlisted_source(self):
        (self.repo / "src/new-module.rs").write_text("pub fn unlisted() {}\n")
        self.reject()

    def test_rejects_stale_lockfile(self):
        path = self.repo / "Cargo.lock"
        path.write_text(path.read_text().replace(TEST_VERSION, "0.0.0"))
        self.reject()

    def test_rejects_missing_vendor_dependency(self):
        shutil.rmtree(self.crate)
        self.reject()

    def test_rejects_extra_vendor_dependency(self):
        (self.vendor / "unlocked-1.0.0").mkdir()
        self.reject()

    def test_rejects_missing_vendor_file(self):
        (self.crate / "build.rs").unlink()
        self.reject()

    def test_rejects_declared_build_file_missing_even_from_checksum_map(self):
        (self.crate / "build.rs").unlink()
        self.refresh_vendor_checksums()
        self.reject()

    def test_rejects_modified_vendor_file(self):
        (self.crate / "src/lib.rs").write_text("modified\n")
        self.reject()

    def test_rejects_unlisted_vendor_file(self):
        (self.crate / "private.key").write_text("do not distribute\n")
        self.reject()

    def test_rejects_vendor_package_checksum_mismatch(self):
        path = self.crate / ".cargo-checksum.json"
        data = json.loads(path.read_bytes())
        data["package"] = "b" * 64
        path.write_bytes(release.canonical_json(data))
        self.reject()

    def test_rejects_escaping_vendor_checksum_path(self):
        path = self.crate / ".cargo-checksum.json"
        data = json.loads(path.read_bytes())
        data["files"]["../../secret"] = "b" * 64
        path.write_bytes(release.canonical_json(data))
        self.reject()

    def test_rejects_nonpublic_registry_source(self):
        path = self.repo / "Cargo.lock"
        path.write_text(path.read_text().replace(release.REGISTRY, "git+https://example.invalid/private"))
        self.reject()

    def test_rejects_escaping_declared_license(self):
        path = self.crate / "Cargo.toml"
        path.write_text(path.read_text().replace('license = "MIT OR Apache-2.0"', 'license-file = "../../secret"'))
        self.refresh_vendor_checksums()
        self.reject()

    def test_rejects_source_file_symlink(self):
        path = self.repo / "README.md"
        path.unlink()
        path.symlink_to(self.crate / "LICENSE-MIT")
        self.reject()

    def test_preserves_alternate_notice_names_and_license_directories(self):
        (self.crate / "UNLICENSE").write_bytes(b"unlicense notice\n")
        (self.crate / "MIT-LICENSE").write_bytes(b"alternate notice\n")
        (self.crate / "LICENSES").mkdir()
        (self.crate / "LICENSES/Apache-2.0.txt").write_bytes(b"nested notice\n")
        self.refresh_vendor_checksums()
        _, _, lock = release.project_snapshot(self.repo)
        _, notices = release.vendor_snapshot(self.vendor, lock)
        for name in ("UNLICENSE", "MIT-LICENSE", "LICENSES/Apache-2.0.txt"):
            self.assertEqual(notices[f"third-party/licenses/fixture-1.0.0/{name}"].bytes(),
                             (self.crate / name).read_bytes())

    def test_rejects_duplicate_checksum_json_keys(self):
        path = self.crate / ".cargo-checksum.json"
        data = path.read_text()
        path.write_text(data.replace('"package":', '"package": "ignored", "package":'))
        self.reject()

    def test_rejects_source_directory_symlink(self):
        path = self.repo / "src/content"
        destination = self.root / "outside-content"
        path.rename(destination)
        path.symlink_to(destination, target_is_directory=True)
        self.reject()

    def test_rejects_vendor_symlink(self):
        path = self.crate / "LICENSE-MIT"
        path.unlink()
        path.symlink_to(self.repo / "LICENSE")
        self.reject()

    def test_rejects_binary_symlink(self):
        path = self.bins / "smolhostd"
        path.unlink()
        path.symlink_to(self.bins / "smolhost")
        self.reject()

    def test_rejects_symlink_input_root(self):
        link = self.root / "linked-vendor"
        link.symlink_to(self.vendor, target_is_directory=True)
        self.vendor = link
        self.reject()

    def test_rejects_special_input_file_without_blocking(self):
        path = self.crate / "build.rs"
        path.unlink()
        os.mkfifo(path)
        self.reject()

    def test_refuses_existing_outputs_without_touching_contents(self):
        self.output.mkdir()
        sentinel = self.output / "keep"
        sentinel.write_bytes(b"preserved")
        with self.assertRaises(release.ReleaseError):
            self.package()
        self.assertEqual(sentinel.read_bytes(), b"preserved")
        self.assertEqual(list(self.output.iterdir()), [sentinel])

    def test_refuses_dangling_output_symlink(self):
        self.output.symlink_to(self.root / "nonexistent")
        with self.assertRaises(release.ReleaseError):
            self.package()
        self.assertTrue(self.output.is_symlink())
        self.assertFalse((self.root / "nonexistent").exists())

    def test_refuses_output_inside_inputs(self):
        with self.assertRaises(release.ReleaseError):
            self.package(self.repo / "artifacts")
        self.assertFalse((self.repo / "artifacts").exists())

    def test_snapshot_rejects_mutated_input_before_archiving(self):
        path = self.repo / "README.md"
        entry = release.Entry.file(path)
        path.write_text("different bytes\n")
        with self.assertRaises(release.ReleaseError):
            release.write_archive(self.root / "changed.tar.gz", {"README.md": entry})

    def test_source_digest_binds_working_tree_contents(self):
        source, _, _ = release.project_snapshot(self.repo)
        before = release.snapshot_digest(source)
        (self.repo / "README.md").write_text("changed public documentation\n")
        source, _, _ = release.project_snapshot(self.repo)
        self.assertNotEqual(before, release.snapshot_digest(source))


if __name__ == "__main__":
    unittest.main()

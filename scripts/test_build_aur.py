#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""AUR generator contracts; no application builds or service operations.

Consumer tests use the shared synthetic archive fixture and real makepkg metadata
generation. They stage files but never execute the fixture binaries.
"""

import hashlib
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("build_aur", Path(__file__).with_name("build-aur.py"))
aur = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(aur)
from test_package_layout import make_archive


class InputTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.archive = self.root / "not-an-archive.tar.gz"
        self.archive.write_bytes(b"not an archive")
        self.output = self.root / "output"

    def test_rejects_malformed_hash_without_creating_output(self):
        for digest in ("", "0" * 63, "g" * 64, "0" * 64 + "\n", "$(touch injected)"):
            with self.subTest(digest=digest), self.assertRaises(aur.AurError):
                aur.generate(self.archive, digest, self.output)
            self.assertFalse(self.output.exists())

    def test_rejects_invalid_revisions_without_creating_output(self):
        for revision in (0, -1, "1.5", "01", "", "1\n", True):
            with self.subTest(revision=revision), self.assertRaises(aur.AurError):
                aur.generate(self.archive, "0" * 64, self.output, revision)
            self.assertFalse(self.output.exists())

    def test_refuses_existing_directory_and_dangling_symlink(self):
        self.output.mkdir()
        sentinel = self.output / "keep"
        sentinel.write_bytes(b"user data")
        with self.assertRaises(aur.AurError):
            aur.generate(self.archive, "0" * 64, self.output)
        self.assertEqual(sentinel.read_bytes(), b"user data")
        link = self.root / "link"
        link.symlink_to(self.root / "missing", target_is_directory=True)
        with self.assertRaises(aur.AurError):
            aur.generate(self.archive, "0" * 64, link)
        self.assertTrue(link.is_symlink())
        self.assertFalse((self.root / "missing").exists())

    def test_refuses_checkout_output_even_through_parent_symlink(self):
        link = self.root / "checkout"
        link.symlink_to(aur.REPO, target_is_directory=True)
        for output in (aur.REPO / "aur-output-not-created", link / "aur-output-not-created"):
            with self.subTest(output=output), self.assertRaises(aur.AurError):
                aur.generate(self.archive, "0" * 64, output)
            self.assertFalse(output.exists())

    @unittest.skipUnless(shutil.which("makepkg"), "requires makepkg metadata tool")
    def test_rejects_archive_pin_mismatch_without_output(self):
        with self.assertRaises(ValueError):
            aur.generate(self.archive, "0" * 64, self.output)
        self.assertFalse(self.output.exists())

    def test_rejects_malformed_or_unsupported_versions(self):
        for version in ("0.1", "01.1.0", "0.1.0-alpha.01", "0.1.0-1", "0.1.0-preview.1",
                        "0.1.0\n", "0.1.0'; touch injected; '"):
            with self.subTest(version=version), self.assertRaises(aur.AurError):
                aur.arch_version(version)

    @unittest.skipUnless(shutil.which("vercmp"), "requires ALPM vercmp")
    def test_prereleases_and_revisions_have_real_alpm_order(self):
        versions = ("0.1.0-alpha.1", "0.1.0-alpha.2", "0.1.0-alpha.10",
                    "0.1.0-beta.1", "0.1.0-rc.1", "0.1.0", "0.1.1-alpha.1")
        for older, newer in zip(versions, versions[1:]):
            result = subprocess.check_output(
                ["vercmp", aur.arch_version(older), aur.arch_version(newer)], text=True)
            self.assertLess(int(result), 0, (older, newer))
        self.assertEqual(aur.arch_version("0.1.0-alpha.1+build.9"), aur.arch_version(versions[0]))
        result = subprocess.check_output(["vercmp", "0.1.0alpha1-1", "0.1.0alpha1-2"], text=True)
        self.assertLess(int(result), 0)

    @unittest.skipUnless(shutil.which("makepkg"), "requires makepkg metadata tool")
    def test_corrupt_archive_is_a_clean_cli_failure(self):
        digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        result = subprocess.run(
            [sys.executable, str(Path(aur.__file__)), "--archive", str(self.archive),
             "--sha256", digest, "--output", str(self.output)],
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertNotIn("Traceback", result.stderr)
        self.assertFalse(self.output.exists())


@unittest.skipUnless(shutil.which("makepkg"), "requires makepkg metadata tool")
class ReleaseConsumerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(cls.temporary.name)
        cls.archive, cls.digest = make_archive(cls.root / "fixture", floor="2.38")
        cls.recipe = aur.generate(cls.archive, cls.digest, cls.root / "recipe")

    def test_metadata_and_both_source_pins_match(self):
        metadata = subprocess.check_output(["makepkg", "--printsrcinfo"], cwd=self.recipe, text=True)
        self.assertEqual((self.recipe / ".SRCINFO").read_text(), metadata)
        fields = {}
        for line in metadata.splitlines():
            if " = " in line:
                key, value = line.strip().split(" = ", 1)
                fields.setdefault(key, []).append(value)
        helper = self.recipe / "package_layout.py"
        self.assertEqual(fields["sha256sums"], [self.digest, hashlib.sha256(helper.read_bytes()).hexdigest()])
        self.assertEqual(helper.read_bytes(), aur.HELPER.read_bytes())
        self.assertEqual(fields["source"][1:], ["package_layout.py"])
        self.assertTrue(fields["source"][0].startswith("https://github.com/replghost/smolhost/releases/download/"))
        self.assertEqual(fields["arch"], ["x86_64"])
        self.assertNotIn("python", fields["depends"])
        self.assertIn("glibc>=2.38", fields["depends"])
        self.assertEqual(set(fields["options"]), {"!strip", "!debug"})

    def test_exported_recipe_stages_without_checkout_dependencies(self):
        detached = self.root / "detached"
        shutil.copytree(self.recipe, detached)
        source = detached / "source"
        manifest = aur.package_layout.extract_bundle(self.archive, source, self.digest)
        # makepkg places local source symlinks beside the extracted archive files.
        (source / "package_layout.py").symlink_to(detached / "package_layout.py")
        archive_name = f"smolhost-v{manifest['version']}-linux-x86_64.tar.gz"
        (source / archive_name).symlink_to(self.archive.resolve())
        destination = detached / "package"
        destination.mkdir()
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        environment["PYTHONNOUSERSITE"] = "1"
        subprocess.run(["bash", "-euc", 'source "$1"; srcdir="$2"; pkgdir="$3"; package',
                        "bash", str(detached / "PKGBUILD"), str(source), str(destination)],
                       cwd=detached, env=environment, capture_output=True, text=True, check=True)
        for name in ("smolhost", "smolhostd"):
            original = source / "bin" / name
            installed = destination / "usr" / "bin" / name
            self.assertEqual(hashlib.sha256(original.read_bytes()).digest(),
                             hashlib.sha256(installed.read_bytes()).digest())
            self.assertEqual(installed.stat().st_mode & 0o777, 0o755)
        self.assertFalse((destination / "home").exists())
        self.assertFalse((destination / "usr/bin/package_layout.py").exists())


if __name__ == "__main__":
    unittest.main()

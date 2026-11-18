#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Generate an unpublished, self-contained AUR source tree (Python 3.11+).

The input is a trusted published binary archive and its independently obtained
SHA256. No downloads, package builds, installation, or publication occur here.
makepkg is required only to generate authoritative .SRCINFO metadata. Output
must be a new directory outside this checkout; package_layout.py is copied there
so AUR consumers do not depend on an unpublished source commit.
"""

import argparse
import hashlib
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile

import package_layout

REPO = Path(__file__).resolve().parent.parent
HELPER = Path(__file__).with_name("package_layout.py")
TEMPLATE = REPO / "packaging" / "aur" / "PKGBUILD.in"
SHA256 = re.compile(r"[0-9a-fA-F]{64}\Z")
# Explicit release channels avoid claiming general SemVer/ALPM equivalence:
# arbitrary SemVer identifiers cannot simply have their separators replaced.
VERSION = re.compile(
    r"(?P<base>(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))"
    r"(?:-(?P<channel>alpha|beta|rc)\.(?P<number>0|[1-9][0-9]*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)


class AurError(ValueError):
    """The inputs cannot safely describe an AUR source package."""


def arch_version(version):
    """Map final/alpha.N/beta.N/rc.N releases to ALPM order; omit build metadata."""
    if not isinstance(version, str) or not (match := VERSION.fullmatch(version)):
        raise AurError("version must be SemVer final, alpha.N, beta.N, or rc.N")
    return match["base"] + (match["channel"] + match["number"] if match["channel"] else "")


def positive_revision(value):
    if not re.fullmatch(r"[1-9][0-9]*", str(value)):
        raise AurError("revision must be a positive integer")
    return int(value)


def generate(archive, sha256, output, revision=1):
    """Validate the archive and create PKGBUILD, .SRCINFO and a pinned helper."""
    if not isinstance(sha256, str) or not SHA256.fullmatch(sha256):
        raise AurError("sha256 must contain exactly 64 hexadecimal characters")
    sha256 = sha256.lower()
    revision = positive_revision(revision)
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise AurError("output must be a new directory")
    output = output.parent.resolve(strict=True) / output.name
    if output == REPO or REPO in output.parents:
        raise AurError("output must be outside the source checkout")
    makepkg = shutil.which("makepkg")
    if makepkg is None:
        raise AurError("makepkg is required to generate .SRCINFO (no package build)")
    with tempfile.TemporaryDirectory(prefix="smolhost-aur-") as temporary:
        manifest = package_layout.extract_bundle(Path(archive), Path(temporary) / "bundle", sha256)
    version = manifest["version"]
    pkgver = arch_version(version)
    archive_name = f"smolhost-v{version}-linux-x86_64.tar.gz"
    archive_url = f"https://github.com/replghost/smolhost/releases/download/v{version}/{archive_name}"
    helper = HELPER.read_bytes()
    recipe = TEMPLATE.read_text(encoding="utf-8")
    values = {
        "PKGVER": pkgver,
        "REVISION": str(revision),
        "ARCHIVE_URL": archive_url,
        "ARCHIVE_SHA256": sha256,
        "HELPER_SHA256": hashlib.sha256(helper).hexdigest(),
        "GLIBC_FLOOR": manifest["glibc_floor"],
    }
    for key, value in values.items():
        recipe = recipe.replace(f"@{key}@", value)
    output.mkdir(mode=0o755)
    try:
        (output / "PKGBUILD").write_text(recipe, encoding="utf-8")
        (output / "package_layout.py").write_bytes(helper)
        result = subprocess.run([makepkg, "--printsrcinfo"], cwd=output,
                                capture_output=True, text=True, check=True)
        (output / ".SRCINFO").write_text(result.stdout, encoding="utf-8")
        for name in ("PKGBUILD", "package_layout.py", ".SRCINFO"):
            (output / name).chmod(0o644)
    except BaseException:
        shutil.rmtree(output)
        raise
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--output", type=Path, required=True, metavar="NEWDIR")
    parser.add_argument("--revision", default="1")
    args = parser.parse_args(argv)
    try:
        destination = generate(args.archive, args.sha256, args.output, args.revision)
    except subprocess.CalledProcessError as error:
        print(f"build-aur: makepkg --printsrcinfo failed: {error.stderr.strip()}", file=sys.stderr)
        return 1
    except (OSError, ValueError, tarfile.TarError) as error:
        print(f"build-aur: {error}", file=sys.stderr)
        return 1
    print(destination)
    return 0


if __name__ == "__main__":
    sys.exit(main())

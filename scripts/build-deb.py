#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Repackage a verified local release as an amd64 deb; Python 3.11+ and dpkg-deb.

No compilation, downloads, binary execution, service changes or publication.
The output directory must not exist, including an empty directory or symlink.
"""

import argparse
import hashlib
from pathlib import Path
import re
import shutil
from string import Template
import subprocess
import sys
import tarfile
import tempfile

import package_layout

REPO = Path(__file__).resolve().parent.parent
METADATA = REPO / "packaging" / "debian"
SEMVER = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
)


def positive_revision(value):
    if not re.fullmatch(r"[1-9][0-9]*", value):
        raise argparse.ArgumentTypeError("revision must be a positive integer")
    return int(value)


def debian_version(version, revision=1):
    """Use Debian's tilde to sort upstream prereleases before the final release."""
    match = SEMVER.fullmatch(version) if isinstance(version, str) else None
    if not match or type(revision) is not int or revision < 1:
        raise ValueError("expected a semantic version and positive integer revision")
    major, minor, patch, prerelease, build = match.groups()
    if prerelease and any(part.isdigit() and len(part) > 1 and part[0] == "0"
                          for part in prerelease.split(".")):
        raise ValueError("numeric prerelease identifiers must not have leading zeros")
    upstream = f"{major}.{minor}.{patch}"
    if prerelease:
        upstream += f"~{prerelease}"
    if build:
        upstream += f"+{build}"
    return f"{upstream}-{revision}"


def glibc_dependency(manifest):
    floor = manifest.get("glibc_floor")
    if not isinstance(floor, str) or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", floor):
        raise ValueError("release manifest has no valid glibc_floor")
    return floor if tuple(map(int, floor.split("."))) > (2, 34) else "2.34"


def write_metadata(staging, manifest, version):
    """Add Debian metadata only; the shared helper owns the entire payload layout."""
    values = {"version": version, "upstream_version": manifest["version"],
              "glibc_floor": glibc_dependency(manifest)}
    copyright_path = staging / "usr/share/doc/smolhost/copyright"
    copyright_text = Template((METADATA / "copyright.in").read_text(encoding="utf-8"))
    with copyright_path.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(copyright_text.substitute(values))
    copyright_path.chmod(0o644)
    files = sorted(path for path in (staging / "usr").rglob("*") if path.is_file())
    values["installed_size"] = (sum(path.stat().st_size for path in files) + 1023) // 1024
    control_dir = staging / "DEBIAN"
    control_dir.mkdir(mode=0o755)
    control_dir.chmod(0o755)
    control = Template((METADATA / "control.in").read_text(encoding="utf-8"))
    with (control_dir / "control").open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(control.substitute(values))
    (control_dir / "control").chmod(0o644)
    with (control_dir / "md5sums").open("x", encoding="utf-8", newline="\n") as stream:
        for path in files:
            with path.open("rb") as payload:
                digest = hashlib.file_digest(payload, "md5").hexdigest()
            stream.write(f"{digest}  {path.relative_to(staging).as_posix()}\n")
    (control_dir / "md5sums").chmod(0o644)


def build_deb(archive, expected_sha256, output, revision=1):
    if type(revision) is not int or revision < 1:
        raise ValueError("revision must be a positive integer")
    if not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256):
        raise ValueError("sha256 must be exactly 64 hexadecimal characters")
    dpkg_deb = shutil.which("dpkg-deb")
    if dpkg_deb is None:
        raise RuntimeError("dpkg-deb is required (install dpkg on the build system)")
    archive = Path(archive).absolute()
    output = Path(output).absolute()
    # Reserve the output namespace exclusively. Never clean up a pre-existing path.
    output.mkdir(mode=0o755)
    try:
        output.chmod(0o755)
        with tempfile.TemporaryDirectory(prefix=".build-", dir=output) as temporary:
            work = Path(temporary)
            bundle, staging = work / "bundle", work / "root"
            manifest = package_layout.extract_bundle(archive, bundle, expected_sha256.lower())
            version = debian_version(manifest["version"], revision)
            glibc_dependency(manifest)
            package_layout.stage_bundle(bundle, staging)
            staging.chmod(0o755)
            write_metadata(staging, manifest, version)
            # GitHub rewrites "~" in asset names; normalize before writing checksums.
            name = f"smolhost_{version.replace('~', '.')}_amd64.deb"
            artifact = output / name
            subprocess.run([dpkg_deb, "--root-owner-group", "-Zxz", "--build",
                            str(staging), str(artifact)], check=True)
            artifact.chmod(0o644)
            with artifact.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            with (output / "SHA256SUMS").open("x", encoding="ascii", newline="\n") as stream:
                stream.write(f"{digest}  {name}\n")
            (output / "SHA256SUMS").chmod(0o644)
    except BaseException:
        shutil.rmtree(output)
        raise
    return artifact


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--sha256", required=True, help="independently verified release archive SHA-256")
    parser.add_argument("--output", required=True, type=Path, help="new directory for deb and SHA256SUMS")
    parser.add_argument("--revision", type=positive_revision, default=1)
    args = parser.parse_args(argv)
    try:
        artifact = build_deb(args.archive, args.sha256, args.output, args.revision)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError,
            subprocess.SubprocessError, tarfile.TarError) as error:
        print(f"build-deb: {error}", file=sys.stderr)
        return 1
    print(artifact)
    print(artifact.parent / "SHA256SUMS")
    return 0


if __name__ == "__main__":
    sys.exit(main())

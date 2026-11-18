#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Stage a verified binary release for native Linux packages; never install it."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tarfile

TARGET = "x86_64-unknown-linux-gnu"
CHAINS = ("paseo.json", "paseo_next_asset_hub.json", "paseo_next_bulletin.json",
          "paseo_next_individuality.json")
MANIFEST = "release-manifest.json"
MAX_BYTES = 256 * 1024 * 1024
MAX_FILES = 10000
HEX256 = re.compile(r"[0-9a-f]{64}\Z")
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9]+(?:\.[A-Za-z0-9]+)*)?\Z")
REQUIRED = {
    "bin/smolhost", "bin/smolhostd", "systemd/smolhost.service", "systemd/smolhost.socket",
    "LICENSE", "README.md", "third-party/dependencies.json", "chains/paseo-next-v2-provenance.json",
    *(f"chains/{name}" for name in CHAINS),
}


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def relative_name(name):
    if not isinstance(name, str) or not name or "\\" in name or "\0" in name:
        raise ValueError("invalid package input path")
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in ("", ".", "..") for part in name.split("/")):
        raise ValueError(f"unsafe package input path: {name!r}")
    return name


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate manifest key: {key}")
        result[key] = value
    return result


def parse_manifest(data):
    manifest = json.loads(data, object_pairs_hook=unique_object)
    if (not isinstance(manifest, dict) or manifest.get("schema_version") != 1
            or manifest.get("kind") != "binary" or manifest.get("name") != "smolhost"
            or manifest.get("target") != TARGET):
        raise ValueError("requires a Smolhost Linux x86_64 binary release manifest")
    version = manifest.get("version")
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise ValueError("unsupported release version")
    floor = manifest.get("glibc_floor")
    if not isinstance(floor, str) or not re.fullmatch(r"[0-9]+\.[0-9]+(?:\.[0-9]+)?", floor):
        raise ValueError("invalid release GLIBC requirement")
    files = manifest.get("files")
    if (not isinstance(files, dict) or not REQUIRED.issubset(files)
            or MANIFEST in files or len(files) > MAX_FILES):
        raise ValueError("incomplete or invalid binary release file inventory")
    total = 0
    for name, metadata in files.items():
        relative_name(name)
        if (not isinstance(metadata, dict) or set(metadata) != {"sha256", "size", "mode"}
                or not isinstance(metadata["sha256"], str) or not HEX256.fullmatch(metadata["sha256"])
                or type(metadata["size"]) is not int or not 0 <= metadata["size"] <= MAX_BYTES
                or metadata["mode"] not in ("0644", "0755")):
            raise ValueError(f"invalid release file metadata: {name}")
        total += metadata["size"]
    if total > MAX_BYTES:
        raise ValueError("binary release exceeds packaging size bound")
    for name in ("bin/smolhost", "bin/smolhostd"):
        if files[name]["mode"] != "0755":
            raise ValueError(f"release binary is not executable: {name}")
    return manifest


def regular_input(root, name):
    path = root
    for part in relative_name(name).split("/"):
        path = path / part
        if path.is_symlink():
            raise ValueError(f"symlinked release input: {path}")
    if not path.is_file():
        raise ValueError(f"missing or non-regular release input: {path}")
    return path


def validate_bundle(bundle):
    if bundle.is_symlink() or not bundle.is_dir():
        raise ValueError("bundle must be a real directory")
    manifest_path = regular_input(bundle, MANIFEST)
    if manifest_path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError("oversized release manifest")
    manifest = parse_manifest(manifest_path.read_bytes())
    for name, metadata in manifest["files"].items():
        path = regular_input(bundle, name)
        info = path.stat()
        if (info.st_size != metadata["size"] or stat.S_IMODE(info.st_mode) != int(metadata["mode"], 8)
                or sha256(path) != metadata["sha256"]):
            raise ValueError(f"release input does not match its manifest: {name}")
    return manifest


def empty_destination(destination):
    if destination.is_symlink():
        raise ValueError("destination must not be a symlink")
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise ValueError("destination must be empty; refusing to overwrite files")


def extract_bundle(archive, destination, expected_sha256):
    """Verify an explicit upstream pin before extracting into an isolated build directory."""
    if not isinstance(expected_sha256, str) or not HEX256.fullmatch(expected_sha256):
        raise ValueError("an explicit SHA-256 archive pin is required")
    if archive.is_symlink() or not archive.is_file():
        raise ValueError("archive must be a regular file")
    with archive.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != expected_sha256:
            raise ValueError("upstream archive SHA-256 mismatch")
        stream.seek(0)
        with tarfile.open(fileobj=stream, mode="r:gz") as tar:
            members, total = {}, 0
            for member in tar:
                name = relative_name(member.name)
                if not member.isfile() or name in members or member.mode not in (0o644, 0o755):
                    raise ValueError(f"unexpected archive member: {name}")
                total += member.size
                if member.size < 0 or total > MAX_BYTES or len(members) >= MAX_FILES:
                    raise ValueError("binary archive exceeds packaging bounds")
                members[name] = member
            if MANIFEST not in members or members[MANIFEST].size > 4 * 1024 * 1024:
                raise ValueError("missing or oversized release manifest")
            manifest = parse_manifest(tar.extractfile(members[MANIFEST]).read())
            if set(members) != set(manifest["files"]) | {MANIFEST}:
                raise ValueError("archive members differ from the release inventory")
            for name, metadata in manifest["files"].items():
                member = members[name]
                if member.size != metadata["size"] or member.mode != int(metadata["mode"], 8):
                    raise ValueError(f"archive metadata mismatch: {name}")
            empty_destination(destination)
            for name, member in members.items():
                path = destination / name
                path.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(member) as source, path.open("xb") as output:
                    shutil.copyfileobj(source, output)
                path.chmod(member.mode)
    return validate_bundle(destination)


def packaged_service(text):
    """Derive the system layout from the released unit, retaining its security/lifecycle policy."""
    replacements = {"smolhostd": "/usr/bin/smolhostd",
                    **{name: f"/usr/share/smolhost/chains/{name}" for name in CHAINS}}
    for name, destination in replacements.items():
        source = f'"%h/.local/lib/smolhost/{name}"'
        if text.count(source) != 1:
            raise ValueError(f"unsupported upstream service layout: {name}")
        text = text.replace(source, f'"{destination}"')
    if "%h/.local/lib/smolhost" in text:
        raise ValueError("unmapped user-local path in upstream service")
    return text


def stage_bundle(bundle, destination):
    """Stage only package-owned /usr files. Never contact systemd or inspect user homes."""
    manifest = validate_bundle(bundle)
    service = packaged_service((bundle / "systemd/smolhost.service").read_text())
    mapping = {
        "bin/smolhost": "usr/bin/smolhost", "bin/smolhostd": "usr/bin/smolhostd",
        "systemd/smolhost.socket": "usr/lib/systemd/user/smolhost.socket",
        "README.md": "usr/share/doc/smolhost/README.md",
        MANIFEST: "usr/share/doc/smolhost/upstream-release-manifest.json",
        "LICENSE": "usr/share/licenses/smolhost/LICENSE",
        "chains/paseo-next-v2-provenance.json": "usr/share/smolhost/chains/paseo-next-v2-provenance.json",
        **{f"chains/{name}": f"usr/share/smolhost/chains/{name}" for name in CHAINS},
        **{name: f"usr/share/licenses/smolhost/{name}"
           for name in manifest["files"] if name.startswith("third-party/")},
    }
    empty_destination(destination)
    for source_name, target_name in mapping.items():
        source = regular_input(bundle, source_name)
        target = destination / target_name
        target.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as stream, target.open("xb") as output:
            shutil.copyfileobj(stream, output)
        target.chmod(0o755 if target_name.startswith("usr/bin/") else 0o644)
        if source_name != MANIFEST and sha256(target) != manifest["files"][source_name]["sha256"]:
            raise ValueError(f"release input changed while staging: {source_name}")
    unit = destination / "usr/lib/systemd/user/smolhost.service"
    with unit.open("x") as output:
        output.write(service)
    unit.chmod(0o644)
    # A caller may build under umask077; package directories still need to be
    # traversable by the normal users who run the service after installation.
    destination.chmod(0o755)
    for path in destination.rglob("*"):
        if path.is_dir():
            path.chmod(0o755)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--dest", type=Path, required=True)
    args = parser.parse_args()
    try:
        manifest = stage_bundle(args.bundle, args.dest)
    except (OSError, ValueError, tarfile.TarError) as error:
        parser.exit(1, f"package-layout: {error}\n")
    print(f"Staged smolhost {manifest['version']} for {TARGET}; no service was started.")


if __name__ == "__main__":
    main()

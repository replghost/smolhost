#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Prepare local, unpublished Linux x86_64 GNU release archives (Python 3.11+).

No downloads, builds, git operations, service changes, or publication. Inputs must
be trusted local build outputs and `cargo vendor --locked --versioned-dirs` output.
The only executed input programs are the two binaries, with --version.
"""

import argparse
from dataclasses import dataclass
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import tomllib

REPO = Path(__file__).resolve().parent.parent
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
TARGET = "x86_64-unknown-linux-gnu"
BINARIES = ("smolhost", "smolhostd")
# Deliberately reviewed, explicit public input list: never archive the worktree.
SOURCE_FILES = tuple(sorted("""
Cargo.toml Cargo.lock README.md LICENSE AGENTS.md .gitignore
src/lib.rs src/client.rs src/discovery.rs src/engine.rs src/ipc.rs
src/platform.rs src/state.rs src/ipc/admission.rs src/ipc/admission/tests.rs
src/content/mod.rs src/content/client.rs src/content/dag.rs
src/bin/smolhost.rs src/bin/smolhostd.rs
examples/fetch-blob.rs examples/read-app-profile.rs examples/watch-balance.rs
examples/watch-recovering.rs examples/app_profile/mod.rs examples/balance/mod.rs
examples/shared/mod.rs examples/shared/metadata.rs examples/shared/tests.rs
examples/recovery_output/mod.rs examples/recovery_output/tests.rs
scripts/content-soak.py scripts/test_content_soak.py scripts/soak.py
scripts/test_soak.py scripts/smoke.py scripts/user-service.py
scripts/test_user_service.py scripts/release.py scripts/test_release.py
scripts/package_layout.py scripts/test_package_layout.py
scripts/build-deb.py scripts/test_build_deb.py
scripts/build-aur.py scripts/test_build_aur.py
packaging/debian/control.in packaging/debian/copyright.in packaging/aur/PKGBUILD.in
chains/paseo.json chains/paseo_asset_hub.json chains/paseo_next_asset_hub.json
chains/paseo_next_bulletin.json chains/paseo_next_individuality.json
chains/paseo-next-v2-provenance.json
systemd/smolhost.service systemd/smolhost.socket
""".split()))
BUNDLE_FILES = tuple(name for name in SOURCE_FILES if
                     name.startswith(("chains/", "systemd/")) or
                     name in ("README.md", "LICENSE", "scripts/user-service.py"))
CARGO_CONFIG = b'''# Relative to the source archive root; no network is required.
[source.crates-io]
replace-with = "vendored-sources"
[source.vendored-sources]
directory = "vendor"
[net]
offline = true
'''
HEX256 = re.compile(r"[0-9a-f]{64}\Z")
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?\Z")
PACKAGE = re.compile(r"[A-Za-z0-9_-]+\Z")
NOTICE = re.compile(r"(?:^|[._-])(?:licen[cs]es?|unlicense|copying|copyright|notices?|authors)(?:$|[._-])", re.I)


class ReleaseError(Exception):
    """Input cannot safely be distributed using this preview's contract."""


def require(condition, message):
    if not condition:
        raise ReleaseError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate vendor checksum JSON key")
        result[key] = value
    return result


def archive_path(name):
    require(isinstance(name, str) and bool(name), "empty archive path")
    path = PurePosixPath(name)
    require(not path.is_absolute() and str(path) == name and
            all(part not in ("", ".", "..") for part in path.parts) and
            not any(ord(char) < 32 or ord(char) == 127 or char in "\\:" for char in name),
            f"unsafe archive path: {name!r}")
    return name


def directory_fd(path):
    """Open every ancestor without following symlinks, including the root input."""
    path = Path(path)
    require(path.is_absolute() and ".." not in path.parts, "paths must be absolute without '..'")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def regular_file(path):
    path = Path(path)
    parent = directory_fd(path.parent)
    try:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    finally:
        os.close(parent)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise ReleaseError(f"not a regular file: {path.name}")
    return os.fdopen(fd, "rb")


def identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def canonical_json(value):
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n").encode()


@dataclass(frozen=True)
class Entry:
    path: Path | None
    data: bytes | None
    sha256: str
    size: int
    mode: int
    stamp: tuple | None = None

    @classmethod
    def file(cls, path, mode=None):
        with regular_file(path) as stream:
            before = os.fstat(stream.fileno())
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
            require(identity(before) == identity(os.fstat(stream.fileno())), "input changed while hashing")
        if mode is None:
            mode = 0o755 if before.st_mode & 0o111 else 0o644
        return cls(Path(path), None, digest, before.st_size, mode, identity(before))

    @classmethod
    def generated(cls, data):
        return cls(None, data, hashlib.sha256(data).hexdigest(), len(data), 0o644)

    def metadata(self):
        return {"sha256": self.sha256, "size": self.size, "mode": f"{self.mode:04o}"}

    def bytes(self):
        if self.data is not None:
            return self.data
        with regular_file(self.path) as stream:
            require(identity(os.fstat(stream.fileno())) == self.stamp, "input changed since snapshot")
            data = stream.read()
        require(hashlib.sha256(data).hexdigest() == self.sha256, "input changed since snapshot")
        return data


def inventory(entries):
    return {name: entry.metadata() for name, entry in sorted(entries.items())}


def snapshot_digest(entries):
    # Unambiguous canonical JSON of relative names, hashes, sizes and normalized modes.
    return hashlib.sha256(canonical_json(inventory(entries))).hexdigest()


def walk_error(error):
    raise error


def tree_files(root):
    """Reject symlinks/special files anywhere in a tree, including directories."""
    fd = directory_fd(root)
    os.close(fd)
    result = []
    for base, directories, files in os.walk(root, followlinks=False, onerror=walk_error):
        for name in sorted(directories + files):
            path = Path(base) / name
            info = path.lstat()
            require(stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode),
                    f"symlink or special file in input tree: {path.name}")
            relative = archive_path(path.relative_to(root).as_posix())
            if stat.S_ISREG(info.st_mode):
                result.append(relative)
    return sorted(result)


def project_snapshot(repo):
    entries = {name: Entry.file(repo / name, 0o755 if name.endswith(".py") else 0o644)
               for name in SOURCE_FILES}
    # Do not silently omit newly added build inputs. Other unlisted files are never included.
    for directory in ("src", "examples"):
        for name in tree_files(repo / directory):
            require(f"{directory}/{name}" in entries, "public source allowlist needs updating")
    for name in ("build.rs", ".cargo/config", ".cargo/config.toml", "rust-toolchain", "rust-toolchain.toml"):
        require(not os.path.lexists(repo / name), f"unsupported additional build input: {name}")
    package = tomllib.loads(entries["Cargo.toml"].bytes().decode())
    lock = tomllib.loads(entries["Cargo.lock"].bytes().decode())
    require(package.get("package", {}).get("name") == "smolhost", "unexpected project package")
    version = package["package"].get("version")
    require(isinstance(version, str) and VERSION.fullmatch(version), "invalid project version")
    require("workspace" not in package and "patch" not in package and "replace" not in package,
            "workspace/patch/replace inputs need explicit release support")
    require(not package["package"].get("build"), "custom build inputs need explicit release support")
    roots = [item for item in lock.get("package", []) if "source" not in item]
    require(len(roots) == 1 and roots[0].get("name") == "smolhost" and
            roots[0].get("version") == version, "Cargo.lock project version is stale or has local dependencies")
    return entries, version, lock


def vendor_snapshot(vendor, lock):
    all_names = tree_files(vendor)
    packages = {}
    for package in lock.get("package", []):
        if "source" not in package:
            continue
        require(package["source"] == REGISTRY, "only public crates.io registry dependencies are supported")
        name, version, checksum = package.get("name"), package.get("version"), package.get("checksum")
        require(isinstance(name, str) and PACKAGE.fullmatch(name) and isinstance(version, str) and
                VERSION.fullmatch(version) and isinstance(checksum, str) and HEX256.fullmatch(checksum),
                "malformed locked dependency")
        key = f"{name}-{version}"
        require(key not in packages, "duplicate locked dependency")
        packages[key] = package
    require(bool(packages), "no locked public dependencies")
    require({path.name for path in vendor.iterdir()} == set(packages) and
            all(path.is_dir() for path in vendor.iterdir()),
            "vendor root must contain exactly the locked versioned package directories")
    require({name.split("/")[0] for name in all_names} == set(packages),
            "vendor tree does not exactly match versioned Cargo.lock dependencies")
    entries, notices, records = {}, {}, []
    for key, package in sorted(packages.items()):
        prefix = key + "/"
        names = [name[len(prefix):] for name in all_names if name.startswith(prefix)]
        require("Cargo.toml" in names and ".cargo-checksum.json" in names,
                f"incomplete vendored package: {key}")
        local = {name: Entry.file(vendor / key / name) for name in names}
        checksums = json.loads(local[".cargo-checksum.json"].bytes(), object_pairs_hook=unique_object)
        require(isinstance(checksums, dict) and checksums.get("package") == package["checksum"] and
                isinstance(checksums.get("files"), dict), f"invalid vendor checksum manifest: {key}")
        require(set(checksums["files"]) == set(names) - {".cargo-checksum.json"},
                f"missing or unlisted vendor files: {key}")
        for name, digest in checksums["files"].items():
            archive_path(name)
            require(isinstance(digest, str) and HEX256.fullmatch(digest) and local[name].sha256 == digest,
                    f"vendor file checksum mismatch: {key}/{name}")
        manifest = tomllib.loads(local["Cargo.toml"].bytes().decode())
        metadata = manifest.get("package", {})
        require(metadata.get("name") == package["name"] and metadata.get("version") == package["version"],
                f"vendor package identity mismatch: {key}")
        targets = ([manifest["lib"]] if "lib" in manifest else [])
        for kind in ("bin", "example", "test", "bench"):
            targets.extend(manifest.get(kind, []))
        required_paths = [target["path"] for target in targets if "path" in target]
        build = metadata.get("build")
        if isinstance(build, str):
            required_paths.append(build)
        elif build is True:
            required_paths.append("build.rs")
        for name in required_paths:
            archive_path(name)
            require(name in local, f"missing declared vendor build input: {key}/{name}")
        license_expression, license_file = metadata.get("license"), metadata.get("license-file")
        require((isinstance(license_expression, str) and bool(license_expression.strip())) or
                (isinstance(license_file, str) and bool(license_file)), f"missing license metadata: {key}")
        selected = {name for name in names if any(NOTICE.search(part) for part in PurePosixPath(name).parts)}
        if license_file:
            archive_path(license_file)
            require(license_file in local, f"missing declared license file: {key}")
            selected.add(license_file)
        for name in names:
            entries[f"vendor/{key}/{name}"] = local[name]
        notice_paths = []
        for name in sorted(selected):
            destination = f"third-party/licenses/{key}/{name}"
            notices[destination] = local[name]
            notice_paths.append(destination)
        records.append({"name": package["name"], "version": package["version"],
                        "source": REGISTRY, "package_checksum": package["checksum"],
                        "license_expression": license_expression, "license_file": license_file,
                        "notice_files": notice_paths})
    notice_manifest = {"schema_version": 1, "scope": "all pinned vendored dependencies, including build/dev/target-specific packages",
                       "disclaimer": "Metadata and verbatim available notices, not a legal audit or a claim that every package is linked.",
                       "packages": records}
    notices["third-party/dependencies.json"] = Entry.generated(canonical_json(notice_manifest))
    return entries, notices


def elf_requirements(data):
    """Read ELF64 dynamic loader/version requirements without external tools."""
    def unpack(fmt, offset):
        size = struct.calcsize(fmt)
        require(0 <= offset <= len(data) - size, "truncated ELF structure")
        return struct.unpack_from(fmt, data, offset)

    require(len(data) >= 64 and data[:7] == b"\x7fELF\x02\x01\x01", "expected little-endian ELF64")
    header = unpack("<HHIQQQIHHHHHH", 16)
    kind, machine, version, _, phoff, _, _, ehsize, phsize, phnum, _, _, _ = header
    require(kind in (2, 3) and machine == 62 and version == 1 and ehsize == 64 and
            phsize == 56 and 0 < phnum < 65535, "expected Linux x86_64 executable ELF")
    require(data[7] in (0, 3), "unsupported ELF OS ABI")
    segments, dynamic, interpreters = [], [], []
    for index in range(phnum):
        ptype, _, offset, address, _, size, memsize, _ = unpack("<IIQQQQQQ", phoff + index * phsize)
        require(offset <= len(data) and size <= len(data) - offset and size <= memsize,
                "invalid ELF segment extent")
        if ptype == 1:
            segments.append((address, offset, size))
        elif ptype == 2:
            dynamic.append((offset, size))
        elif ptype == 3:
            interpreters.append(data[offset:offset + size])
    require(interpreters == [b"/lib64/ld-linux-x86-64.so.2\0"] and len(dynamic) == 1,
            "expected GNU Linux dynamic loader")

    def address_offset(address, size):
        matches = [offset + address - base for base, offset, length in segments
                   if base <= address and address - base + size <= length]
        require(len(matches) == 1, "ELF virtual address is not uniquely file-backed")
        return matches[0]

    tags, needed, terminated = {}, [], False
    offset, length = dynamic[0]
    require(length % 16 == 0, "invalid ELF dynamic table")
    for position in range(offset, offset + length, 16):
        tag, value = unpack("<qQ", position)
        if tag == 0:
            terminated = True
            break
        if tag == 1:
            needed.append(value)
        else:
            require(tag not in tags or tags[tag] == value, "conflicting ELF dynamic entries")
            tags[tag] = value
    require(terminated and all(tag in tags for tag in (5, 10, 0x6ffffffe, 0x6fffffff)),
            "missing ELF dynamic version requirements")
    strings_offset = address_offset(tags[5], tags[10])
    strings = data[strings_offset:strings_offset + tags[10]]

    def string(index):
        require(0 <= index < len(strings), "invalid ELF string offset")
        end = strings.find(b"\0", index)
        require(end >= 0, "unterminated ELF string")
        try:
            return strings[index:end].decode("ascii")
        except UnicodeDecodeError as error:
            raise ReleaseError("non-ASCII ELF dependency name") from error

    libraries = sorted({string(index) for index in needed})
    require("libc.so.6" in libraries and all("/" not in name for name in libraries),
            "missing libc or path-qualified dynamic dependency")
    require(15 not in tags and 29 not in tags, "RPATH/RUNPATH is not supported in release binaries")
    count = tags[0x6fffffff]
    require(0 < count <= 4096, "invalid ELF version requirement count")
    current, seen, versions = tags[0x6ffffffe], set(), set()
    for index in range(count):
        require(current not in seen, "cyclic ELF version requirements")
        seen.add(current)
        revision, auxiliary_count, library, auxiliary, following = unpack("<HHIII", address_offset(current, 16))
        require(revision == 1 and 0 < auxiliary_count <= 4096 and auxiliary >= 16 and
                string(library) in libraries, "invalid ELF version requirement")
        aux_address, aux_seen = current + auxiliary, set()
        for aux_index in range(auxiliary_count):
            require(aux_address not in aux_seen, "cyclic ELF auxiliary versions")
            aux_seen.add(aux_address)
            _, _, _, name, next_aux = unpack("<IHHII", address_offset(aux_address, 16))
            requirement = string(name)
            if requirement.startswith("GLIBC_"):
                value = requirement.removeprefix("GLIBC_")
                require(re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", value), "unsupported nonnumeric GLIBC requirement")
                versions.add(tuple(int(part) for part in value.split(".")))
            require((next_aux >= 16) if aux_index + 1 < auxiliary_count else next_aux == 0,
                    "invalid ELF auxiliary version chain")
            aux_address += next_aux
        require((following >= 16) if index + 1 < count else following == 0,
                "invalid ELF version requirement chain")
        current += following
    require(bool(versions), "no dynamic GLIBC requirement found")
    floor = max(versions)
    require(floor <= (2, 39), "binary requires GLIBC newer than Ubuntu 24.04 (2.39)")
    return {"glibc_floor": ".".join(map(str, floor)), "needed_libraries": libraries,
            "interpreter": "/lib64/ld-linux-x86-64.so.2"}


def binary_snapshot(bin_dir, version, source):
    entries, records = {}, {}
    for name in BINARIES:
        other_bins = {f"src/bin/{other}.rs" for other in BINARIES if other != name}
        newest_source = max(entry.stamp[3] for path, entry in source.items()
                            if (path.startswith("src/") and path not in other_bins)
                            or path in ("Cargo.toml", "Cargo.lock"))
        path = bin_dir / name
        entry = Entry.file(path, 0o755)
        require(entry.stamp[3] >= newest_source, f"binary predates Rust source or Cargo inputs: {name}")
        with regular_file(path) as stream:
            require(os.fstat(stream.fileno()).st_mode & 0o111, f"binary is not executable: {name}")
        requirements = elf_requirements(entry.bytes())
        result = subprocess.run([str(path), "--version"], capture_output=True, text=True,
                                timeout=15, check=False)
        require(result.returncode == 0 and result.stdout.strip() == f"{name} {version}" and
                not result.stderr.strip(), f"binary version mismatch: {name}")
        # Detect replacement or mutation while invoking --version, before any output is created.
        require(Entry.file(path, 0o755) == entry, f"binary changed during inspection: {name}")
        entries[f"bin/{name}"] = entry
        records[name] = {**entry.metadata(), **requirements, "version": version}
    return entries, records


class HashingReader:
    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()

    def read(self, size=-1):
        data = self.stream.read(size)
        self.digest.update(data)
        return data


def write_archive(destination, entries):
    for name in entries:
        archive_path(name)
        for parent in PurePosixPath(name).parents:
            require(str(parent) not in entries, "archive file/directory collision")
    with destination.open("xb") as output:
        with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0, compresslevel=9) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as archive:
                for name, entry in sorted(entries.items()):
                    info = tarfile.TarInfo(name)
                    info.size, info.mode = entry.size, entry.mode
                    info.uid = info.gid = info.mtime = 0
                    info.uname = info.gname = ""
                    if entry.data is not None:
                        archive.addfile(info, io.BytesIO(entry.data))
                    else:
                        with regular_file(entry.path) as stream:
                            require(identity(os.fstat(stream.fileno())) == entry.stamp,
                                    "input changed before archiving")
                            reader = HashingReader(stream)
                            archive.addfile(info, reader)
                            require(reader.digest.hexdigest() == entry.sha256 and
                                    identity(os.fstat(stream.fileno())) == entry.stamp,
                                    "input changed while archiving")


def with_manifest(entries, common, kind):
    require("release-manifest.json" not in entries, "release metadata collision")
    manifest = {**common, "kind": kind, "files": inventory(entries)}
    return {**entries, "release-manifest.json": Entry.generated(canonical_json(manifest))}


def package_release(repo, bin_dir, vendor_dir, output):
    repo, bin_dir, vendor_dir, output = map(Path, (repo, bin_dir, vendor_dir, output))
    for path in (repo, bin_dir, vendor_dir, output.parent):
        fd = directory_fd(path)
        os.close(fd)
    require(output.is_absolute() and ".." not in output.parts, "output must be an absolute new directory")
    require(not os.path.lexists(output), "refusing to overwrite an existing output")
    require(not any(output.is_relative_to(path) for path in (repo, bin_dir, vendor_dir)),
            "output must be outside all input trees")
    source, version, lock = project_snapshot(repo)
    vendor, notices = vendor_snapshot(vendor_dir, lock)
    binaries, binary_records = binary_snapshot(bin_dir, version, source)
    common = {
        "schema_version": 1, "name": "smolhost", "version": version,
        "status": "experimental-preview", "target": TARGET,
        "source_snapshot": {"algorithm": "sha256-canonical-json-file-inventory-v1",
                            "sha256": snapshot_digest(source), "files": inventory(source),
                            "scope": "explicit public source allowlist; exact working-tree bytes, no claimed published commit"},
        "vendor_snapshot_sha256": snapshot_digest(vendor),
        "binaries": binary_records,
        "glibc_floor": max((record["glibc_floor"] for record in binary_records.values()),
                           key=lambda value: tuple(map(int, value.split(".")))),
        "compatibility": {"glibc_ceiling": "2.39", "baseline": "Ubuntu 24.04 x86_64 GNU",
                          "qualification": "ELF inspection only; packaging does not establish runtime qualification"},
        "source_build": {"command": "cargo build --offline --locked --release --bins",
                         "working_directory": "source archive root", "toolchain": "Rust/Cargo 1.98.1",
                         "target": TARGET,
                         "prerequisites": ["Linux x86_64 GNU", "installed Rust 1.98.1 toolchain and standard library",
                                           "C compiler, linker, libc development headers and GNU make",
                                           "Python 3.11+ for installer/release scripts"],
                         "network": "not required; Cargo sources replaced by relative vendor directory"},
        "install_command": "python3 scripts/user-service.py install --bin-dir ./bin",
        "provenance_limits": ["Version and input timestamps reject obvious stale binaries but do not prove binary/source correspondence.",
                              "Hashes bind supplied source and binaries; no reproducible-build or clean-commit claim.",
                              "Vendor file hashes and package checksums are checked against Cargo.lock; the original registry archives are not re-downloaded or independently authenticated.",
                              "License metadata/notices are not a legal audit.",
                              "No archive, tag, or release has been published by this tool."],
    }
    binary_entries = {**binaries, **{name: source[name] for name in BUNDLE_FILES}, **notices}
    source_entries = {**source, **vendor, **notices, ".cargo/config.toml": Entry.generated(CARGO_CONFIG)}
    archives = {
        f"smolhost-v{version}-linux-x86_64.tar.gz": with_manifest(binary_entries, common, "binary"),
        f"smolhost-v{version}-source.tar.gz": with_manifest(source_entries, common, "corresponding-source"),
    }
    # Exclusive creation reserves the entire output namespace; cleanup only our own new directory.
    output.mkdir(mode=0o755)
    try:
        for name, entries in sorted(archives.items()):
            write_archive(output / name, entries)
        artifacts = {name: Entry.file(output / name, 0o644).metadata() for name in sorted(archives)}
        artifact_manifest = {"schema_version": 1, "name": "smolhost", "version": version,
                             "status": common["status"], "target": TARGET,
                             "source_snapshot_sha256": common["source_snapshot"]["sha256"],
                             "vendor_snapshot_sha256": common["vendor_snapshot_sha256"],
                             "glibc_floor": common["glibc_floor"], "artifacts": artifacts,
                             "checksums": "SHA256SUMS"}
        with (output / "artifact-manifest.json").open("xb") as stream:
            stream.write(canonical_json(artifact_manifest))
        checksums = {**artifacts, "artifact-manifest.json": Entry.file(output / "artifact-manifest.json").metadata()}
        with (output / "SHA256SUMS").open("x", encoding="ascii", newline="\n") as stream:
            stream.writelines(f"{metadata['sha256']}  {name}\n" for name, metadata in sorted(checksums.items()))
    except BaseException:
        shutil.rmtree(output)
        raise
    return artifact_manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--vendor-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        manifest = package_release(REPO, args.bin_dir, args.vendor_dir, args.output)
    except (ReleaseError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"release: {error}", file=sys.stderr)
        return 1
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

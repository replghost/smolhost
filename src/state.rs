// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    env,
    fs::{self, File, Metadata},
    io::{BufWriter, Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    },
    path::PathBuf,
};

use anyhow::{Context, Result, ensure};
use rustix::fs::{FlockOperation, Mode, OFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_DATABASE_BYTES: usize = 8 * 1024 * 1024;
const MAX_ENVELOPE_BYTES: usize = MAX_DATABASE_BYTES * 6 + 4096;
const FORMAT: u32 = 1;
const ENGINE: &str = "smoldot-light/4.0.0";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    format: u32,
    engine: T,
    specification_sha256: T,
    database_sha256: T,
    database: T,
}

pub fn default_state_dir() -> Result<PathBuf> {
    if let Some(path) = env::var_os("XDG_STATE_HOME") {
        let path = PathBuf::from(path);
        if path.is_absolute() {
            return Ok(path.join("polkadot-host"));
        }
    }
    let home = PathBuf::from(env::var_os("HOME").context("HOME is not set")?);
    ensure!(home.is_absolute(), "HOME must be nonempty and absolute");
    Ok(home.join(".local/state/polkadot-host"))
}

/// Owns a per-specification process lock until dropped. Lock files are never unlinked.
pub struct StateStore {
    directory: File,
    specification_sha256: String,
    checkpoint_name: String,
    _lock: File,
}

impl StateStore {
    pub fn open(directory: PathBuf, specification: &str) -> Result<Self> {
        ensure!(directory.is_absolute(), "state directory must be absolute");
        // Remove trailing separators and `.` so O_NOFOLLOW applies to the leaf.
        let directory: PathBuf = directory.components().collect();
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&directory)
            .context("creating private state directory")?;
        validate_directory(&fs::symlink_metadata(&directory)?)?;
        let directory = File::from(
            rustix::fs::open(
                &directory,
                OFlags::RDONLY
                    | OFlags::DIRECTORY
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK
                    | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .context("opening state directory")?,
        );
        validate_directory(&directory.metadata()?)?;
        let specification_sha256 = digest(specification.as_bytes());
        let checkpoint_name = format!("{specification_sha256}.json");
        let lock_name = format!("{specification_sha256}.lock");
        let lock = File::from(
            rustix::fs::openat(
                &directory,
                &lock_name,
                OFlags::RDWR
                    | OFlags::CREATE
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK
                    | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )
            .context("opening state lock file")?,
        );
        validate_file(&lock.metadata()?).context("invalid state lock file")?;
        rustix::fs::flock(&lock, FlockOperation::NonBlockingLockExclusive)
            .context("locking state (another daemon may already own this specification)")?;
        Ok(Self {
            directory,
            specification_sha256,
            checkpoint_name,
            _lock: lock,
        })
    }

    pub fn load(&self) -> Result<Option<String>> {
        let Some(file) = self.open_checkpoint()? else {
            return Ok(None);
        };
        let length = file.metadata()?.len();
        ensure!(
            length <= MAX_ENVELOPE_BYTES as u64,
            "checkpoint envelope exceeds size limit"
        );
        let mut bytes = Vec::with_capacity(length as usize);
        file.take(MAX_ENVELOPE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .context("reading checkpoint")?;
        ensure!(
            bytes.len() <= MAX_ENVELOPE_BYTES,
            "checkpoint envelope exceeds size limit"
        );
        let envelope: Envelope<String> =
            serde_json::from_slice(&bytes).context("decoding checkpoint envelope")?;
        ensure!(envelope.format == FORMAT, "unsupported checkpoint format");
        ensure!(envelope.engine == ENGINE, "checkpoint engine mismatch");
        ensure!(
            envelope.specification_sha256 == self.specification_sha256,
            "checkpoint specification mismatch"
        );
        ensure!(
            envelope.database.len() <= MAX_DATABASE_BYTES,
            "checkpoint database exceeds size limit"
        );
        ensure!(
            envelope.database_sha256 == digest(envelope.database.as_bytes()),
            "checkpoint database checksum mismatch"
        );
        // The pinned engine validates locally generated snapshots before saving.
        Ok(Some(envelope.database))
    }

    /// Atomically publishes a checkpoint, then syncs the directory entry.
    /// A directory-sync error can occur after the new checkpoint is already visible.
    pub fn save(&self, database: &str) -> Result<()> {
        ensure!(
            database.len() <= MAX_DATABASE_BYTES,
            "checkpoint database exceeds size limit"
        );
        // Reject an unsafe or unreadable existing path rather than replacing it.
        self.open_checkpoint()?;
        let database_sha256 = digest(database.as_bytes());
        let envelope = Envelope {
            format: FORMAT,
            engine: ENGINE,
            specification_sha256: self.specification_sha256.as_str(),
            database_sha256: database_sha256.as_str(),
            database,
        };
        // Linux procfs anchors tempfile's path-based API to our open directory,
        // even if an ancestor or the original directory pathname is renamed.
        let directory = self.anchored_directory();
        let mut temporary = tempfile::Builder::new()
            .prefix(".checkpoint-")
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(&directory)
            .context("creating private checkpoint temporary file")?;
        // A restrictive umask must not publish a file that load would reject.
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .context("setting checkpoint temporary file permissions")?;
        {
            let mut writer = BufWriter::new(temporary.as_file_mut());
            serde_json::to_writer(&mut writer, &envelope).context("serializing checkpoint")?;
            writer.flush().context("flushing checkpoint data")?;
        }
        temporary
            .as_file()
            .sync_all()
            .context("syncing checkpoint data")?;
        // Recheck after serialization in case the destination changed meanwhile.
        self.open_checkpoint()?;
        temporary
            .persist(directory.join(&self.checkpoint_name))
            .context("publishing checkpoint atomically")?;
        self.directory
            .sync_all()
            .context("syncing state directory after checkpoint publication")?;
        Ok(())
    }

    fn open_checkpoint(&self) -> Result<Option<File>> {
        let fd = match rustix::fs::openat(
            &self.directory,
            &self.checkpoint_name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(error) => return Err(error).context("opening checkpoint"),
        };
        let file = File::from(fd);
        validate_file(&file.metadata()?).context("invalid checkpoint file")?;
        Ok(Some(file))
    }

    fn anchored_directory(&self) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.directory.as_raw_fd()))
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validate_directory(metadata: &Metadata) -> Result<()> {
    ensure!(
        metadata.is_dir()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.mode() & 0o7777 == 0o700,
        "state directory must be a real directory owned by this user with mode 0700"
    );
    Ok(())
}

fn validate_file(metadata: &Metadata) -> Result<()> {
    ensure!(
        metadata.is_file()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.mode() & 0o7777 == 0o600,
        "state file must be a regular file owned by this user with mode 0600"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    const SPEC: &str = "test specification bytes";

    fn checkpoint(directory: &std::path::Path) -> PathBuf {
        directory.join(format!("{}.json", digest(SPEC.as_bytes())))
    }

    fn lockfile(directory: &std::path::Path) -> PathBuf {
        directory.join(format!("{}.lock", digest(SPEC.as_bytes())))
    }

    fn private_directory() -> tempfile::TempDir {
        tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap()
    }

    fn private_file(path: &std::path::Path, bytes: &[u8]) {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        std::io::Write::write_all(&mut file, bytes).unwrap();
    }

    #[test]
    fn replacement_is_atomic_for_existing_readers() {
        let directory = private_directory();
        let store = StateStore::open(directory.path().to_owned(), SPEC).unwrap();
        assert_eq!(store.load().unwrap(), None);
        store.save("old database").unwrap();
        let path = checkpoint(directory.path());
        let old_bytes = fs::read(&path).unwrap();
        let mut reader = File::open(&path).unwrap();
        store.save("new database\n\"escaped\"\u{0}").unwrap();
        let mut still_old = Vec::new();
        reader.read_to_end(&mut still_old).unwrap();
        assert_eq!(still_old, old_bytes);
        assert_ne!(
            reader.metadata().unwrap().ino(),
            fs::metadata(&path).unwrap().ino()
        );
        assert_eq!(
            store.load().unwrap().as_deref(),
            Some("new database\n\"escaped\"\u{0}")
        );
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 2);
    }

    #[test]
    fn failed_saves_preserve_existing_checkpoint() {
        let directory = private_directory();
        let store = StateStore::open(directory.path().to_owned(), SPEC).unwrap();
        store.save("original").unwrap();
        let path = checkpoint(directory.path());
        let original = fs::read(&path).unwrap();
        assert!(store.save(&"x".repeat(MAX_DATABASE_BYTES + 1)).is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
        assert!(store.save("replacement").is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o400);
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 2);
    }

    #[test]
    fn corrupt_and_foreign_envelopes_are_errors_and_untouched() {
        let directory = private_directory();
        let store = StateStore::open(directory.path().to_owned(), SPEC).unwrap();
        store.save("opaque engine database").unwrap();
        let path = checkpoint(directory.path());
        let valid: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let mut bad_envelopes = vec![b"not JSON".to_vec(), b"{}".to_vec()];
        for (field, value) in [
            ("format", serde_json::json!(2)),
            ("engine", serde_json::json!("smoldot-light/other")),
            (
                "specification_sha256",
                serde_json::json!(digest(b"different specification")),
            ),
            (
                "database_sha256",
                serde_json::json!(digest(b"different database")),
            ),
            ("database", serde_json::json!("changed database")),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            bad_envelopes.push(serde_json::to_vec(&invalid).unwrap());
        }
        for bytes in bad_envelopes {
            fs::write(&path, &bytes).unwrap();
            assert!(store.load().is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn oversized_envelope_and_database_are_rejected_untouched() {
        let directory = private_directory();
        let store = StateStore::open(directory.path().to_owned(), SPEC).unwrap();
        let path = checkpoint(directory.path());
        let database = "x".repeat(MAX_DATABASE_BYTES + 1);
        let checksum = digest(database.as_bytes());
        let bytes = serde_json::to_vec(&Envelope {
            format: FORMAT,
            engine: ENGINE,
            specification_sha256: store.specification_sha256.as_str(),
            database_sha256: checksum.as_str(),
            database: database.as_str(),
        })
        .unwrap();
        private_file(&path, &bytes);
        assert!(store.load().is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(MAX_ENVELOPE_BYTES as u64 + 1).unwrap();
        assert!(store.load().is_err());
        assert_eq!(
            file.metadata().unwrap().len(),
            MAX_ENVELOPE_BYTES as u64 + 1
        );
    }

    #[test]
    fn lock_is_exclusive_released_on_drop_and_never_unlinked() {
        let directory = private_directory();
        let first = StateStore::open(directory.path().to_owned(), SPEC).unwrap();
        let path = lockfile(directory.path());
        let inode = fs::metadata(&path).unwrap().ino();
        assert!(StateStore::open(directory.path().to_owned(), SPEC).is_err());
        // Fingerprints are of the exact bytes, not parsed/normalized specifications.
        let different = StateStore::open(directory.path().to_owned(), &format!("{SPEC} ")).unwrap();
        drop(different);
        drop(first);
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
        let reopened = StateStore::open(directory.path().to_owned(), SPEC).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
        drop(reopened);
    }

    #[test]
    fn unsafe_directories_are_rejected_without_changes() {
        let root = tempfile::tempdir().unwrap();
        let shared = root.path().join("shared");
        fs::create_dir(&shared).unwrap();
        fs::set_permissions(&shared, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(StateStore::open(shared.clone(), SPEC).is_err());
        assert_eq!(fs::metadata(&shared).unwrap().mode() & 0o777, 0o755);
        assert_eq!(fs::read_dir(&shared).unwrap().count(), 0);
        let linked = root.path().join("linked");
        symlink(root.path(), &linked).unwrap();
        assert!(StateStore::open(linked.clone(), SPEC).is_err());
        assert!(StateStore::open(linked.join(""), SPEC).is_err());
        assert!(
            fs::symlink_metadata(&linked)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        let regular = root.path().join("regular");
        private_file(&regular, b"untouched");
        assert!(StateStore::open(regular.clone(), SPEC).is_err());
        assert_eq!(fs::read(regular).unwrap(), b"untouched");
    }

    #[test]
    fn missing_parent_directories_are_private() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = first.join("second");
        let leaf = second.join("state");
        let _store = StateStore::open(leaf.clone(), SPEC).unwrap();
        for path in [first, second, leaf] {
            assert_eq!(fs::metadata(path).unwrap().mode() & 0o777, 0o700);
        }
    }

    #[test]
    fn unsafe_checkpoint_paths_are_rejected_without_changes() {
        let root = private_directory();
        let store = StateStore::open(root.path().to_owned(), SPEC).unwrap();
        let path = checkpoint(root.path());
        let target = root.path().join("unrelated");
        private_file(&target, b"unrelated bytes");
        symlink(&target, &path).unwrap();
        assert!(store.load().is_err());
        assert!(store.save("replacement").is_err());
        assert!(
            fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&target).unwrap(), b"unrelated bytes");
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(store.load().is_err());
        assert!(store.save("replacement").is_err());
        assert!(path.is_dir());
        fs::remove_dir(&path).unwrap();
        rustix::fs::mknodat(
            &store.directory,
            &store.checkpoint_name,
            rustix::fs::FileType::Fifo,
            Mode::RUSR | Mode::WUSR,
            0,
        )
        .unwrap();
        // O_NONBLOCK makes both calls fail promptly, rather than hanging on a FIFO.
        assert!(store.load().is_err());
        assert!(store.save("replacement").is_err());
        use std::os::unix::fs::FileTypeExt;
        assert!(fs::symlink_metadata(&path).unwrap().file_type().is_fifo());
        fs::remove_file(&path).unwrap();
        private_file(&path, b"private bytes");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(store.load().is_err());
        assert!(store.save("replacement").is_err());
        assert_eq!(fs::read(&path).unwrap(), b"private bytes");
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o644);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        assert!(store.load().is_err());
        assert!(store.save("replacement").is_err());
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o000);
    }

    #[test]
    fn unsafe_lockfiles_are_rejected_without_changes() {
        let root = tempfile::tempdir().unwrap();
        let path = lockfile(root.path());
        let target = root.path().join("unrelated");
        private_file(&target, b"unrelated lock target");
        symlink(&target, &path).unwrap();
        assert!(StateStore::open(root.path().to_owned(), SPEC).is_err());
        assert!(
            fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&target).unwrap(), b"unrelated lock target");
        fs::remove_file(&path).unwrap();
        private_file(&path, b"existing lock bytes");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(StateStore::open(root.path().to_owned(), SPEC).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"existing lock bytes");
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o644);
    }
}

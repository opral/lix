//! Same-directory staging for atomic mirror replacement, without an fsync promise.
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

const PREFIX: &str = ".lix-mirror-";
const SUFFIX: &str = ".tmp";
const RANDOM_LEN: usize = 16;

// Independently opened adapters can scan the same directory in this process.
// Register creation and cleanup under one lock so a scan cannot unlink a live writer.
static ACTIVE: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Mutex::default);

pub(crate) fn is_staging_name(name: &OsStr) -> bool {
    name.to_str()
        .and_then(|name| name.strip_prefix(PREFIX))
        .and_then(|name| name.strip_suffix(SUFFIX))
        .is_some_and(|random| {
            random.len() == RANDOM_LEN && random.bytes().all(|b| b.is_ascii_alphanumeric())
        })
}

pub(crate) fn cleanup_abandoned(path: &Path) {
    if !path.file_name().is_some_and(is_staging_name) {
        return;
    }
    let active = ACTIVE.lock().unwrap_or_else(|error| error.into_inner());
    if active.contains(path) {
        return;
    }
    // Never follow or remove a symlink or directory matching the reserved pattern.
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file()) {
        let _ = fs::remove_file(path);
    }
}

struct StagedFile {
    file: Option<tempfile::NamedTempFile>,
    path: PathBuf,
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        let mut active = ACTIVE.lock().unwrap_or_else(|error| error.into_inner());
        // Remove the file before making its name eligible for abandoned cleanup.
        drop(self.file.take());
        active.remove(&self.path);
    }
}

pub(crate) fn atomic_write(path: &Path, data: &[u8]) -> io::Result<()> {
    atomic_write_with(path, |file| file.write_all(data))
}

fn atomic_write_with(
    path: &Path,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("mirror file has no parent"))?;
    let permissions = match fs::metadata(path) {
        Ok(metadata) => Some(metadata.permissions()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let mut staged = {
        let mut active = ACTIVE.lock().unwrap_or_else(|error| error.into_inner());
        let file = tempfile::Builder::new()
            .prefix(PREFIX)
            .suffix(SUFFIX)
            .rand_bytes(RANDOM_LEN)
            // Match ordinary file creation permissions (including the process umask).
            .make_in(parent, |path| {
                let mut options = fs::OpenOptions::new();
                options.write(true).create_new(true);
                // Never expose an existing private file through a broader staging mode.
                #[cfg(unix)]
                if permissions.is_some() {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                options.open(path)
            })?;
        let path = file.path().to_path_buf();
        active.insert(path.clone());
        StagedFile {
            file: Some(file),
            path,
        }
    };
    let file = staged.file.as_mut().expect("staging file exists");
    write(file.as_file_mut())?;
    if let Some(permissions) = permissions {
        file.as_file().set_permissions(permissions)?;
    }
    // TempPath::persist uses the platform's replacing rename. Close the writer first;
    // no file/directory fsync is performed: this is visibility atomicity, not durability.
    let file = staged.file.take().expect("staging file exists");
    file.into_temp_path()
        .persist(path)
        .map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_partial_write_preserves_destination_and_cleans_staging() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        let result = atomic_write_with(&path, |file| {
            file.write_all(b"partial")?;
            assert_eq!(fs::read(&path)?, b"original");
            Err(io::Error::other("injected write failure"))
        });
        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"original");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn replacement_publishes_only_completed_contents_and_retains_live_staging() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        atomic_write_with(&path, |file| {
            file.write_all(b"first")?;
            for entry in fs::read_dir(dir.path())? {
                cleanup_abandoned(&entry?.path());
            }
            assert_eq!(fs::read(&path)?, b"original");
            file.write_all(b"second")
        })
        .unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"firstsecond");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        atomic_write(&path, b"").unwrap();
        assert!(fs::read(&path).unwrap().is_empty());
    }

    #[test]
    fn interrupted_writer_child() {
        let Some(root) = std::env::var_os("LIX_TEST_INTERRUPTED_MIRROR_ROOT") else {
            return;
        };
        atomic_write_with(&PathBuf::from(root).join("file"), |file| {
            file.write_all(b"partial")?;
            // Simulate termination without running Rust destructors.
            std::process::exit(73);
        })
        .unwrap();
    }

    #[test]
    fn process_interruption_keeps_old_file_and_abandoned_stage_is_removable() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("file"), b"original").unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "atomic_write::tests::interrupted_writer_child",
                "--nocapture",
            ])
            .env("LIX_TEST_INTERRUPTED_MIRROR_ROOT", dir.path())
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(73),
            "child must execute the interruption hook: {result:?}"
        );
        assert_eq!(fs::read(dir.path().join("file")).unwrap(), b"original");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
        for entry in fs::read_dir(dir.path()).unwrap() {
            cleanup_abandoned(&entry.unwrap().path());
        }
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn rename_failure_preserves_destination_and_cleans_staging() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("directory");
        fs::create_dir(&path).unwrap();
        assert!(atomic_write(&path, b"data").is_err());
        assert!(path.is_dir());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn abandoned_cleanup_only_removes_reserved_regular_files() {
        let dir = tempfile::tempdir().unwrap();
        let abandoned = dir.path().join(".lix-mirror-0123456789abcdef.tmp");
        fs::write(&abandoned, b"partial").unwrap();
        cleanup_abandoned(&abandoned);
        assert!(!abandoned.exists());
        let ordinary = dir.path().join(".lix-mirror-not-reserved.tmp");
        fs::write(&ordinary, b"user data").unwrap();
        cleanup_abandoned(&ordinary);
        assert!(ordinary.exists());
        fs::create_dir(&abandoned).unwrap();
        cleanup_abandoned(&abandoned);
        assert!(abandoned.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn private_contents_remain_private_during_staging_and_after_failure() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("private");
        fs::write(&path, b"secret").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let result = atomic_write_with(&path, |file| {
            file.write_all(b"new secret")?;
            assert_eq!(file.metadata()?.permissions().mode() & 0o077, 0);
            Err(io::Error::other("injected failure"))
        });
        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"secret");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn replacement_preserves_permissions_and_does_not_mutate_open_readers() {
        use std::io::Read;
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        fs::write(&path, b"original").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o751)).unwrap();
        let mut reader = File::open(&path).unwrap();
        atomic_write(&path, b"replacement").unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o751
        );
        let mut old = String::new();
        reader.read_to_string(&mut old).unwrap();
        assert_eq!(old, "original");
        assert_eq!(fs::read(&path).unwrap(), b"replacement");
    }
}

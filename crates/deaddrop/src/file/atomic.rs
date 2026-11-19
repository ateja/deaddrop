//! Exclusive create and no-replace rename, per platform.

#[cfg(unix)]
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;

/// Creates `path` exclusively (`O_CREAT|O_EXCL` / `CREATE_NEW`), writes
/// `bytes` and optionally flushes them to stable storage.
pub(crate) fn write_new_file(
    path: &Path,
    bytes: &[u8],
    durable: bool,
    #[cfg_attr(not(unix), allow(unused_variables))] unix_mode: u32,
    #[cfg_attr(not(unix), allow(unused_variables))] unix_group: Option<u32>,
) -> io::Result<()> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(unix_mode)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = opts.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // The open mode is filtered by umask; set it exactly.
        file.set_permissions(std::fs::Permissions::from_mode(unix_mode))?;
        if let Some(gid) = unix_group {
            std::os::unix::fs::fchown(&file, None, Some(gid))?;
        }
    }
    file.write_all(bytes)?;
    if durable {
        file.sync_all()?;
    }
    Ok(())
}

/// Atomically renames `from` to `to`, failing with `AlreadyExists` rather
/// than replacing an existing `to`.
pub(crate) fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
    imp::rename_no_replace(from, to)
}

/// Persists the directory entry created by the rename.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        // NTFS journals the rename itself; MOVEFILE_WRITE_THROUGH already
        // waited for it. Opening a directory handle for flushing needs
        // backup semantics and buys nothing here.
        let _ = dir;
        Ok(())
    }
}

#[cfg(unix)]
mod imp {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    fn cpath(p: &Path) -> io::Result<CString> {
        CString::new(p.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
    }

    /// Portable no-replace: `link(2)` fails with EEXIST and is atomic, then
    /// drop the temp name. Used where no native flag exists.
    fn link_then_unlink(from: &Path, to: &Path) -> io::Result<()> {
        std::fs::hard_link(from, to)?;
        std::fs::remove_file(from)
    }

    fn unsupported(err: &io::Error) -> bool {
        matches!(
            err.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ENOSYS) | Some(libc::ENOTSUP)
        )
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub(super) fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
        let (f, t) = (cpath(from)?, cpath(to)?);
        // SAFETY: both pointers are valid NUL terminated C strings.
        let rc = unsafe { libc::renamex_np(f.as_ptr(), t.as_ptr(), libc::RENAME_EXCL) };
        if rc == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if unsupported(&err) {
            return link_then_unlink(from, to);
        }
        Err(err)
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    pub(super) fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
        let (f, t) = (cpath(from)?, cpath(to)?);
        // SAFETY: both pointers are valid NUL terminated C strings.
        let rc = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                f.as_ptr(),
                libc::AT_FDCWD,
                t.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if rc == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if unsupported(&err) {
            return link_then_unlink(from, to);
        }
        Err(err)
    }

    #[cfg(not(any(
        target_os = "macos",
        target_os = "ios",
        all(target_os = "linux", target_env = "gnu")
    )))]
    pub(super) fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
        let _ = cpath;
        let _ = unsupported;
        link_then_unlink(from, to)
    }
}

#[cfg(windows)]
mod imp {
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;

    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

    pub(super) fn wide(p: &Path) -> Vec<u16> {
        p.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    pub(super) fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
        let (f, t) = (wide(from), wide(to));
        // Without MOVEFILE_REPLACE_EXISTING the call fails with
        // ERROR_ALREADY_EXISTS instead of clobbering. Same volume rename is
        // atomic on NTFS.
        // SAFETY: both buffers are NUL terminated UTF-16 and outlive the call.
        let ok = unsafe { MoveFileExW(f.as_ptr(), t.as_ptr(), MOVEFILE_WRITE_THROUGH) };
        if ok != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_to_clobber() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        write_new_file(&a, b"first", false, 0o644, None).unwrap();
        assert_eq!(
            write_new_file(&a, b"again", false, 0o644, None)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        write_new_file(&b, b"second", false, 0o644, None).unwrap();
        let err = rename_no_replace(&a, &b).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&b).unwrap(), b"second");

        let c = dir.path().join("c");
        rename_no_replace(&a, &c).unwrap();
        assert!(!a.exists());
        assert_eq!(std::fs::read(&c).unwrap(), b"first");
    }
}

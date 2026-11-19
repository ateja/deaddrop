//! Directory creation with restrictive permissions, and the trust checks a
//! consumer runs before reading anything.

use std::path::Path;

use crate::{Error, ProducerConfig, Result, TrustedOwner};

/// Creates the directory if missing, then verifies it. The verification runs
/// even after creation, so losing a creation race to an attacker is caught.
pub(crate) fn prepare_producer_dir(config: &ProducerConfig) -> Result<()> {
    let path = config.dir.as_path();
    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }
            imp::create_secure_dir(config)?;
        }
        Err(e) => return Err(e.into()),
    }
    imp::verify_dir(path, &[TrustedOwner::CurrentUser, TrustedOwner::Privileged])
}

/// Verifies the directory a consumer is about to trust.
pub(crate) fn verify_consumer_dir(path: &Path, owner: &TrustedOwner) -> Result<()> {
    imp::verify_dir(path, std::slice::from_ref(owner))
}

#[cfg(unix)]
mod imp {
    use std::fs;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    use std::path::Path;

    use super::*;

    fn euid() -> u32 {
        // SAFETY: geteuid has no preconditions and cannot fail.
        unsafe { libc::geteuid() }
    }

    pub(super) fn create_secure_dir(config: &ProducerConfig) -> Result<()> {
        let path = config.dir.as_path();
        match fs::DirBuilder::new().mode(config.dir_mode).create(path) {
            Ok(()) => {
                fs::set_permissions(path, fs::Permissions::from_mode(config.dir_mode))?;
                if let Some(gid) = config.group {
                    std::os::unix::fs::chown(path, None, Some(gid))?;
                }
                Ok(())
            }
            // Someone else created it first; verify_dir decides if we trust it.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn owner_ok(uid: u32, trusted: &TrustedOwner) -> bool {
        match trusted {
            TrustedOwner::Privileged => uid == 0,
            TrustedOwner::CurrentUser => uid == euid(),
            TrustedOwner::Uid(expected) => uid == *expected,
            TrustedOwner::Any => true,
        }
    }

    pub(super) fn verify_dir(path: &Path, trusted: &[TrustedOwner]) -> Result<()> {
        let meta = fs::symlink_metadata(path)?;
        if meta.file_type().is_symlink() {
            return Err(Error::insecure(path, "is a symbolic link"));
        }
        if !meta.is_dir() {
            return Err(Error::insecure(path, "is not a directory"));
        }
        let mode = meta.mode() & 0o7777;
        if mode & 0o022 != 0 {
            return Err(Error::insecure(
                path,
                format!(
                    "mode {mode:o} lets group or others write; expected no group/other write bits"
                ),
            ));
        }
        let uid = meta.uid();
        if !trusted.iter().any(|t| owner_ok(uid, t)) {
            return Err(Error::insecure(
                path,
                format!("owned by uid {uid}, which is not a trusted owner ({trusted:?})"),
            ));
        }
        Ok(())
    }
}

#[cfg(windows)]
mod imp {
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;
    use std::ptr;

    use windows_sys::Win32::Foundation::{
        CloseHandle, GetLastError, LocalFree, ERROR_ALREADY_EXISTS, ERROR_SUCCESS, HANDLE,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, GetNamedSecurityInfoW,
        SDDL_REVISION_1, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::{
        EqualSid, GetTokenInformation, IsWellKnownSid, TokenUser, WinBuiltinAdministratorsSid,
        WinLocalSystemSid, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
        SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateDirectoryW, GetFileAttributesW, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_REPARSE_POINT, INVALID_FILE_ATTRIBUTES,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    use super::*;

    fn wide(p: &Path) -> Vec<u16> {
        p.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    pub(super) fn create_secure_dir(config: &ProducerConfig) -> Result<()> {
        let sddl: Vec<u16> = config
            .dir_sddl
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        let mut sd: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: sddl is NUL terminated; sd receives a LocalAlloc'd buffer.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(Error::InvalidConfig(format!(
                "dir_sddl rejected: {}",
                std::io::Error::last_os_error()
            )));
        }
        let attrs = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: sd,
            bInheritHandle: 0,
        };
        let path = wide(&config.dir);
        // SAFETY: path is NUL terminated, attrs points at a valid descriptor.
        let created = unsafe { CreateDirectoryW(path.as_ptr(), &attrs) };
        let err = unsafe { GetLastError() };
        // SAFETY: sd came from LocalAlloc inside the conversion call.
        unsafe { LocalFree(sd as _) };
        if created == 0 && err != ERROR_ALREADY_EXISTS {
            return Err(std::io::Error::from_raw_os_error(err as i32).into());
        }
        Ok(())
    }

    /// Owner SID of the current process token, as an owned byte buffer.
    /// Returned as `u64`s so the `TOKEN_USER` header is pointer aligned.
    fn current_user_sid() -> Result<Vec<u64>> {
        let mut token: HANDLE = ptr::null_mut();
        // SAFETY: GetCurrentProcess returns a pseudo handle; token is an out param.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut len = 0u32;
        // First call sizes the buffer.
        unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut len) };
        let mut buf = vec![0u64; (len as usize).div_ceil(8)];
        let ok = unsafe {
            GetTokenInformation(token, TokenUser, buf.as_mut_ptr().cast(), len, &mut len)
        };
        unsafe { CloseHandle(token) };
        if ok == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(buf)
    }

    fn owner_ok(owner: PSID, trusted: &TrustedOwner, me: &mut Option<Vec<u64>>) -> Result<bool> {
        // SAFETY: owner points into a live security descriptor.
        let privileged = || unsafe {
            IsWellKnownSid(owner, WinLocalSystemSid) != 0
                || IsWellKnownSid(owner, WinBuiltinAdministratorsSid) != 0
        };
        Ok(match trusted {
            TrustedOwner::Privileged => privileged(),
            TrustedOwner::CurrentUser => {
                if me.is_none() {
                    *me = Some(current_user_sid()?);
                }
                let buf = me.as_ref().expect("populated above");
                // SAFETY: buf holds a TOKEN_USER written by GetTokenInformation.
                let user = unsafe { &*(buf.as_ptr() as *const TOKEN_USER) };
                // Elevated admins create objects owned by Administrators.
                let is_me = unsafe { EqualSid(owner, user.User.Sid) != 0 };
                is_me || privileged()
            }
            TrustedOwner::Any => true,
        })
    }

    pub(super) fn verify_dir(path: &Path, trusted: &[TrustedOwner]) -> Result<()> {
        let wpath = wide(path);
        // SAFETY: wpath is NUL terminated.
        let attrs = unsafe { GetFileAttributesW(wpath.as_ptr()) };
        if attrs == INVALID_FILE_ATTRIBUTES {
            return Err(std::io::Error::last_os_error().into());
        }
        if attrs & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(Error::insecure(
                path,
                "is a reparse point (symlink or junction)",
            ));
        }
        if attrs & FILE_ATTRIBUTE_DIRECTORY == 0 {
            return Err(Error::insecure(path, "is not a directory"));
        }

        let mut owner: PSID = ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: out params are valid; sd must be LocalFree'd.
        let rc = unsafe {
            GetNamedSecurityInfoW(
                wpath.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION,
                &mut owner,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                &mut sd,
            )
        };
        if rc != ERROR_SUCCESS {
            return Err(std::io::Error::from_raw_os_error(rc as i32).into());
        }
        let mut me = None;
        let mut verdict = Ok(false);
        for t in trusted {
            match owner_ok(owner, t, &mut me) {
                Ok(true) => {
                    verdict = Ok(true);
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    verdict = Err(e);
                    break;
                }
            }
        }
        // SAFETY: sd was allocated by GetNamedSecurityInfoW.
        unsafe { LocalFree(sd as _) };
        if !verdict? {
            return Err(Error::insecure(
                path,
                format!("owner is not a trusted account ({trusted:?})"),
            ));
        }
        Ok(())
    }
}

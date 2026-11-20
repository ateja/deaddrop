//! Windows named pipes.
//!
//! The consumer owns `\\.\pipe\<name>`: byte mode, inbound only, local
//! clients only, created with `FILE_FLAG_FIRST_PIPE_INSTANCE` so a second
//! process cannot take over the name while the consumer runs. The default
//! pipe DACL lets SYSTEM, Administrators and the consumer's own account
//! write; everyone else may only read, which is useless for an inbound pipe.
//! On top of that, each side checks the account of the process on the other
//! end before an event crosses.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, RawHandle};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, LocalFree, ERROR_PIPE_BUSY, ERROR_PIPE_CONNECTED, ERROR_SUCCESS,
    HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo, SDDL_REVISION_1,
    SE_KERNEL_OBJECT,
};
use windows_sys::Win32::Security::{
    EqualSid, GetTokenInformation, IsWellKnownSid, TokenOwner, TokenUser,
    WinBuiltinAdministratorsSid, WinLocalSystemSid, OWNER_SECURITY_INFORMATION,
    PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES, TOKEN_OWNER, TOKEN_QUERY, TOKEN_USER,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_FIRST_PIPE_INSTANCE, PIPE_ACCESS_INBOUND, SECURITY_IDENTIFICATION,
};
use windows_sys::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, DisconnectNamedPipe, GetNamedPipeClientProcessId,
    WaitNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE,
    PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
};
use windows_sys::Win32::System::IO::CancelSynchronousIo;

use super::stream::{read_frames, write_frame, MAGIC};
use super::{Link, Server, Sink};
use crate::logging::CONSUMER;
use crate::{Error, Result, TrustedOwner};

const BUFFER_BYTES: u32 = 64 * 1024;
/// Lets any authenticated local user write, for `trusted_peer: any`.
const ANY_USER_SDDL: &str = "D:(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)(A;;GRGW;;;AU)";

pub(super) fn pipe_path(name: &str) -> String {
    format!(r"\\.\pipe\{name}")
}

pub(super) fn endpoint(name: &str) -> String {
    format!("named pipe {}", pipe_path(name))
}

fn wide(s: &str) -> Vec<u16> {
    std::ffi::OsStr::new(s)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

/// Closes a HANDLE on drop.
struct OwnedHandle(HANDLE);
impl Drop for OwnedHandle {
    fn drop(&mut self) {
        // SAFETY: we own the handle.
        unsafe { CloseHandle(self.0) };
    }
}

/// `GetTokenInformation` into a pointer aligned buffer.
fn token_info(token: HANDLE, class: i32) -> io::Result<Vec<u64>> {
    let mut len = 0u32;
    // SAFETY: sizing call with a null buffer.
    unsafe { GetTokenInformation(token, class, ptr::null_mut(), 0, &mut len) };
    let mut buf = vec![0u64; (len as usize).div_ceil(8).max(1)];
    // SAFETY: buffer is at least `len` bytes.
    if unsafe { GetTokenInformation(token, class, buf.as_mut_ptr().cast(), len, &mut len) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(buf)
}

/// The user SID of the current process, owned.
fn current_user() -> io::Result<Vec<u64>> {
    let mut token: HANDLE = ptr::null_mut();
    // SAFETY: pseudo handle for the current process; token is an out param.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = OwnedHandle(token);
    token_info(token.0, TokenUser)
}

fn sid_is_privileged(sid: PSID) -> bool {
    // SAFETY: sid points into a live buffer or descriptor.
    unsafe {
        IsWellKnownSid(sid, WinLocalSystemSid) != 0
            || IsWellKnownSid(sid, WinBuiltinAdministratorsSid) != 0
    }
}

fn sid_is_current_user(sid: PSID) -> io::Result<bool> {
    let me = current_user()?;
    // SAFETY: `me` holds a TOKEN_USER written by GetTokenInformation.
    let me = unsafe { &*(me.as_ptr() as *const TOKEN_USER) };
    // SAFETY: both SIDs are valid.
    Ok(unsafe { EqualSid(sid, me.User.Sid) } != 0)
}

/// Checks the account a process runs as. Elevated administrators count as
/// privileged: their token's default owner is BUILTIN\Administrators.
fn process_trusted(pid: u32, policy: &TrustedOwner) -> io::Result<bool> {
    if matches!(policy, TrustedOwner::Any) {
        return Ok(true);
    }
    // SAFETY: plain Win32 calls; handles are closed by OwnedHandle.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    let process = OwnedHandle(process);
    let mut token: HANDLE = ptr::null_mut();
    if unsafe { OpenProcessToken(process.0, TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = OwnedHandle(token);
    let user_buf = token_info(token.0, TokenUser)?;
    let owner_buf = token_info(token.0, TokenOwner)?;
    // SAFETY: buffers hold the structures GetTokenInformation wrote.
    let user = unsafe { &*(user_buf.as_ptr() as *const TOKEN_USER) }
        .User
        .Sid;
    let owner = unsafe { &*(owner_buf.as_ptr() as *const TOKEN_OWNER) }.Owner;
    let privileged = sid_is_privileged(user) || sid_is_privileged(owner);
    Ok(match policy {
        TrustedOwner::Privileged => privileged,
        TrustedOwner::CurrentUser => privileged || sid_is_current_user(user)?,
        TrustedOwner::Any => true,
    })
}

/// Checks who created the pipe (its owner) without opening the server
/// process, which an unprivileged producer may not be allowed to do.
fn pipe_owner_trusted(pipe: HANDLE, policy: &TrustedOwner) -> io::Result<bool> {
    if matches!(policy, TrustedOwner::Any) {
        return Ok(true);
    }
    let mut owner: PSID = ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: out params are valid; sd is freed below.
    let rc = unsafe {
        GetSecurityInfo(
            pipe,
            SE_KERNEL_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut owner,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut sd,
        )
    };
    if rc != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(rc as i32));
    }
    let verdict = match policy {
        TrustedOwner::Privileged => Ok(sid_is_privileged(owner)),
        TrustedOwner::CurrentUser => {
            sid_is_current_user(owner).map(|me| me || sid_is_privileged(owner))
        }
        TrustedOwner::Any => Ok(true),
    };
    // SAFETY: allocated by GetSecurityInfo.
    unsafe { LocalFree(sd as _) };
    verdict
}

// ------------------------------------------------------------------ producer

struct PipeLink {
    file: File,
    peer: String,
}

impl Link for PipeLink {
    fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        write_frame(&mut self.file, payload)
    }
    fn peer(&self) -> &str {
        &self.peer
    }
}

pub(super) fn connect(name: &str, policy: &TrustedOwner) -> io::Result<Box<dyn Link>> {
    let path = pipe_path(name);
    let mut opts = OpenOptions::new();
    // Identification level: the consumer can learn who we are but cannot
    // impersonate us.
    opts.write(true).security_qos_flags(SECURITY_IDENTIFICATION);
    let mut file = None;
    for _ in 0..3 {
        match opts.open(&path) {
            Ok(f) => {
                file = Some(f);
                break;
            }
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                // Every instance is busy for a moment; wait for a free one.
                // SAFETY: NUL terminated path.
                unsafe { WaitNamedPipeW(wide(&path).as_ptr(), 1000) };
            }
            Err(e) => return Err(e),
        }
    }
    let mut file =
        file.ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "pipe stayed busy"))?;
    let handle = file.as_raw_handle() as HANDLE;
    if !pipe_owner_trusted(handle, policy)? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{path} was created by an account that is not a trusted consumer ({policy:?})"),
        ));
    }
    io::Write::write_all(&mut file, MAGIC)?;
    Ok(Box::new(PipeLink {
        file,
        peer: format!("consumer on {path}"),
    }))
}

// ------------------------------------------------------------------ consumer

/// Raw handles are pointers; keep them as integers so threads can share them.
type HandleValue = usize;

struct PipeServer {
    path: String,
    stop: Arc<AtomicBool>,
    acceptor: Option<JoinHandle<()>>,
    readers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

fn create_instance(
    path: &[u16],
    first: bool,
    sa: Option<&SECURITY_ATTRIBUTES>,
) -> io::Result<HANDLE> {
    let open_mode = PIPE_ACCESS_INBOUND
        | if first {
            FILE_FLAG_FIRST_PIPE_INSTANCE
        } else {
            0
        };
    // SAFETY: NUL terminated name; optional security attributes outlive the call.
    let h = unsafe {
        CreateNamedPipeW(
            path.as_ptr(),
            open_mode,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
            PIPE_UNLIMITED_INSTANCES,
            0,
            BUFFER_BYTES,
            0,
            sa.map_or(ptr::null(), |s| s as *const _),
        )
    };
    if h == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    Ok(h)
}

/// Security attributes for `policy`, or None for the default DACL.
struct Security {
    sd: PSECURITY_DESCRIPTOR,
    attrs: SECURITY_ATTRIBUTES,
}
unsafe impl Send for Security {}
impl Security {
    fn for_policy(policy: &TrustedOwner) -> io::Result<Option<Self>> {
        if !matches!(policy, TrustedOwner::Any) {
            return Ok(None);
        }
        let sddl = wide(ANY_USER_SDDL);
        let mut sd: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: NUL terminated SDDL; sd is LocalAlloc'd and freed in Drop.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Some(Self {
            sd,
            attrs: SECURITY_ATTRIBUTES {
                nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: sd,
                bInheritHandle: 0,
            },
        }))
    }
}
impl Drop for Security {
    fn drop(&mut self) {
        // SAFETY: allocated by the conversion call.
        unsafe { LocalFree(self.sd as _) };
    }
}

pub(super) fn serve(
    name: &str,
    policy: TrustedOwner,
    max_bytes: usize,
    sink: Sink,
) -> Result<Box<dyn Server>> {
    let path = pipe_path(name);
    let wpath = wide(&path);
    let security = Security::for_policy(&policy)?;
    // Create the first instance here so a taken name fails start_watching().
    let first =
        create_instance(&wpath, true, security.as_ref().map(|s| &s.attrs)).map_err(|e| {
            Error::Ipc(format!(
                "cannot create {path}: {e} (another consumer may already own this name)"
            ))
        })?;

    let stop = Arc::new(AtomicBool::new(false));
    let readers: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
    let acceptor = {
        let (stop, readers) = (Arc::clone(&stop), Arc::clone(&readers));
        let first = first as HandleValue;
        let path = path.clone();
        thread::Builder::new()
            .name("deaddrop-ipc-accept".into())
            .spawn(move || {
                let security = security; // keep the descriptor alive
                let mut pending = first as HANDLE;
                loop {
                    // stop() may have run while no instance existed for its
                    // wake-up connection to land on; check before blocking.
                    if stop.load(Ordering::Acquire) {
                        unsafe { CloseHandle(pending) };
                        break;
                    }
                    // SAFETY: valid pipe instance; blocking connect.
                    let ok = unsafe { ConnectNamedPipe(pending, ptr::null_mut()) } != 0
                        || unsafe { GetLastError() } == ERROR_PIPE_CONNECTED;
                    if stop.load(Ordering::Acquire) {
                        unsafe { CloseHandle(pending) };
                        break;
                    }
                    if ok {
                        accept(pending, &path, &policy, max_bytes, &sink, &readers);
                    } else {
                        log::warn!(target: CONSUMER, "ConnectNamedPipe failed: {}", io::Error::last_os_error());
                        unsafe { CloseHandle(pending) };
                    }
                    // Next instance for the next producer.
                    pending = loop {
                        match create_instance(&wpath, false, security.as_ref().map(|s| &s.attrs)) {
                            Ok(h) => break h,
                            Err(e) => {
                                if stop.load(Ordering::Acquire) {
                                    return;
                                }
                                log::warn!(target: CONSUMER, "cannot create another instance of {path}: {e}; retrying");
                                thread::sleep(std::time::Duration::from_millis(200));
                            }
                        }
                    };
                }
            })?
    };
    Ok(Box::new(PipeServer {
        path,
        stop,
        acceptor: Some(acceptor),
        readers,
    }))
}

fn accept(
    pipe: HANDLE,
    path: &str,
    policy: &TrustedOwner,
    max_bytes: usize,
    sink: &Sink,
    readers: &Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    let mut pid = 0u32;
    // SAFETY: connected pipe instance.
    unsafe { GetNamedPipeClientProcessId(pipe, &mut pid) };
    let peer = format!("producer pid {pid}");
    match process_trusted(pid, policy) {
        Ok(true) => {}
        Ok(false) => {
            log::warn!(target: CONSUMER, "rejected connection to {path} from {peer}: not {policy:?}");
            unsafe {
                DisconnectNamedPipe(pipe);
                CloseHandle(pipe);
            }
            return;
        }
        Err(e) => {
            log::warn!(target: CONSUMER, "rejected connection to {path} from {peer}: cannot verify its account ({e})");
            unsafe {
                DisconnectNamedPipe(pipe);
                CloseHandle(pipe);
            }
            return;
        }
    }
    log::debug!(target: CONSUMER, "accepted connection on {path} from {peer}");
    let sink = sink.clone();
    let handle = pipe as HandleValue;
    let reader = thread::Builder::new()
        .name("deaddrop-ipc-reader".into())
        .spawn(move || {
            // SAFETY: we own this pipe instance from here on.
            let file = unsafe { File::from_raw_handle(handle as RawHandle) };
            read_frames(&file, &peer, max_bytes, &sink);
            drop(file); // closes this pipe instance
        });
    match reader {
        Ok(h) => readers.lock().unwrap_or_else(|p| p.into_inner()).push(h),
        Err(e) => {
            log::warn!(target: CONSUMER, "cannot start reader thread: {e}");
            unsafe { CloseHandle(pipe) };
        }
    }
}

impl Server for PipeServer {
    fn stop(&mut self) {
        let Some(acceptor) = self.acceptor.take() else {
            return;
        };
        self.stop.store(true, Ordering::Release);
        // Wake the blocking ConnectNamedPipe with a throwaway client.
        let _ = OpenOptions::new().write(true).open(&self.path);
        let _ = acceptor.join();

        // Readers block in ReadFile on synchronous handles. Windows serializes
        // I/O per synchronous handle, so DisconnectNamedPipe or CloseHandle
        // from here would wait behind that read forever. CancelSynchronousIo
        // aborts the read instead; repeat it, since a reader may be between
        // two reads when one cancel lands.
        let mut readers: Vec<_> = self
            .readers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain(..)
            .collect();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while !readers.is_empty() && std::time::Instant::now() < deadline {
            for r in &readers {
                if !r.is_finished() {
                    // SAFETY: the JoinHandle keeps the thread handle valid.
                    unsafe { CancelSynchronousIo(r.as_raw_handle() as HANDLE) };
                }
            }
            thread::sleep(std::time::Duration::from_millis(5));
            let (done, pending): (Vec<_>, Vec<_>) =
                readers.into_iter().partition(|r| r.is_finished());
            for r in done {
                let _ = r.join();
            }
            readers = pending;
        }
        if !readers.is_empty() {
            log::warn!(target: CONSUMER, "{} reader thread(s) on {} did not stop in time; detaching", readers.len(), self.path);
        }
    }
}

impl Drop for PipeServer {
    fn drop(&mut self) {
        self.stop();
    }
}

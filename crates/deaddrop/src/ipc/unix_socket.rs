//! Unix domain sockets (Linux and other non-macOS Unix).

use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use super::stream::{read_frames, write_frame, MAGIC};
use super::{Link, Server, Sink};
use crate::logging::CONSUMER;
use crate::{Error, Result, TrustedOwner};

/// `/tmp/<name>.sock`. A fixed directory (not `$XDG_RUNTIME_DIR`) so a root
/// producer and a user consumer agree on the path.
pub(super) fn socket_path(name: &str) -> PathBuf {
    Path::new("/tmp").join(format!("{name}.sock"))
}

pub(super) fn endpoint(name: &str) -> String {
    format!("Unix socket {}", socket_path(name).display())
}

use super::{euid, uid_trusted};

/// (uid, pid) of the process on the other end. pid is -1 where unknown.
fn peer_identity(stream: &UnixStream) -> io::Result<(u32, i32)> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let mut cred = libc::ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        // SAFETY: valid fd, out buffer of the size we pass.
        let rc = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast(),
                &mut len,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((cred.uid, cred.pid))
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let (mut uid, mut gid) = (0, 0);
        // SAFETY: valid fd and out pointers.
        if unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((uid, -1))
    }
}

fn describe_peer(role: &str, uid: u32, pid: i32) -> String {
    if pid > 0 {
        format!("{role} pid {pid} uid {uid}")
    } else {
        format!("{role} uid {uid}")
    }
}

// ------------------------------------------------------------------ producer

struct UnixLink {
    stream: UnixStream,
    peer: String,
}

impl Link for UnixLink {
    fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        write_frame(&mut self.stream, payload)
    }
    fn peer(&self) -> &str {
        &self.peer
    }
}

pub(super) fn connect(name: &str, policy: &TrustedOwner) -> io::Result<Box<dyn Link>> {
    let path = socket_path(name);
    let mut stream = UnixStream::connect(&path)?;
    let (uid, pid) = peer_identity(&stream)?;
    if !uid_trusted(uid, policy) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "the process listening on {} ({}) is not a trusted consumer ({policy:?})",
                path.display(),
                describe_peer("consumer", uid, pid)
            ),
        ));
    }
    io::Write::write_all(&mut stream, MAGIC)?;
    Ok(Box::new(UnixLink {
        stream,
        peer: describe_peer("consumer", uid, pid),
    }))
}

// ------------------------------------------------------------------ consumer

struct UnixServer {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    acceptor: Option<JoinHandle<()>>,
    conns: Arc<Mutex<HashMap<u64, UnixStream>>>,
    readers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

/// Removes a stale socket left by a crashed consumer; refuses anything else.
fn clear_stale_socket(path: &Path) -> Result<()> {
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    if !meta.file_type().is_socket() {
        return Err(Error::insecure(path, "exists and is not a socket"));
    }
    if UnixStream::connect(path).is_ok() {
        return Err(Error::Ipc(format!(
            "another consumer is already listening on {}",
            path.display()
        )));
    }
    if meta.uid() != euid() && euid() != 0 {
        return Err(Error::insecure(
            path,
            format!(
                "stale socket owned by uid {}; remove it manually",
                meta.uid()
            ),
        ));
    }
    log::debug!(target: CONSUMER, "removing stale socket {} left by an earlier run", path.display());
    fs::remove_file(path)?;
    Ok(())
}

pub(super) fn serve(
    name: &str,
    policy: TrustedOwner,
    max_bytes: usize,
    sink: Sink,
) -> Result<Box<dyn Server>> {
    let path = socket_path(name);
    clear_stale_socket(&path)?;
    let listener = UnixListener::bind(&path)?;
    // Only the accounts the policy allows need to connect at all.
    let mode = match policy {
        TrustedOwner::Uid(_) | TrustedOwner::Any => 0o666,
        TrustedOwner::Privileged | TrustedOwner::CurrentUser => 0o600,
    };
    fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;

    let stop = Arc::new(AtomicBool::new(false));
    let conns: Arc<Mutex<HashMap<u64, UnixStream>>> = Arc::default();
    let readers: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
    let acceptor = {
        let (stop, conns, readers) = (Arc::clone(&stop), Arc::clone(&conns), Arc::clone(&readers));
        let next_id = AtomicU64::new(0);
        thread::Builder::new()
            .name("deaddrop-ipc-accept".into())
            .spawn(move || {
                for conn in listener.incoming() {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let stream = match conn {
                        Ok(s) => s,
                        Err(e) => {
                            log::warn!(target: CONSUMER, "accept failed: {e}");
                            continue;
                        }
                    };
                    let (uid, pid) = match peer_identity(&stream) {
                        Ok(id) => id,
                        Err(e) => {
                            log::warn!(target: CONSUMER, "cannot identify connecting process: {e}; closing");
                            continue;
                        }
                    };
                    let peer = describe_peer("producer", uid, pid);
                    if !uid_trusted(uid, &policy) {
                        log::warn!(target: CONSUMER, "rejected connection from {peer}: not {policy:?}");
                        continue;
                    }
                    log::debug!(target: CONSUMER, "accepted connection from {peer}");
                    let id = next_id.fetch_add(1, Ordering::Relaxed);
                    if let Ok(clone) = stream.try_clone() {
                        conns.lock().unwrap_or_else(|p| p.into_inner()).insert(id, clone);
                    }
                    let (sink, conns2) = (sink.clone(), Arc::clone(&conns));
                    let reader = thread::Builder::new()
                        .name("deaddrop-ipc-reader".into())
                        .spawn(move || {
                            read_frames(&stream, &peer, max_bytes, &sink);
                            conns2.lock().unwrap_or_else(|p| p.into_inner()).remove(&id);
                        });
                    match reader {
                        Ok(h) => readers.lock().unwrap_or_else(|p| p.into_inner()).push(h),
                        Err(e) => log::warn!(target: CONSUMER, "cannot start reader thread: {e}"),
                    }
                }
            })?
    };
    Ok(Box::new(UnixServer {
        path,
        stop,
        acceptor: Some(acceptor),
        conns,
        readers,
    }))
}

impl Server for UnixServer {
    fn stop(&mut self) {
        let Some(acceptor) = self.acceptor.take() else {
            return;
        };
        self.stop.store(true, Ordering::Release);
        // Wake the blocking accept() with a throwaway connection.
        let _ = UnixStream::connect(&self.path);
        let _ = acceptor.join();
        for (_, s) in self.conns.lock().unwrap_or_else(|p| p.into_inner()).drain() {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
        let readers: Vec<_> = self
            .readers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain(..)
            .collect();
        for r in readers {
            let _ = r.join();
        }
        let _ = fs::remove_file(&self.path);
    }
}

impl Drop for UnixServer {
    fn drop(&mut self) {
        self.stop();
    }
}

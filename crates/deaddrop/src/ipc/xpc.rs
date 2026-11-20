//! macOS XPC, through libxpc's C API (part of libSystem).
//!
//! The consumer listens on the Mach service `<name>`; producers connect to
//! it and send each event as a dictionary `{ "event": <JSON bytes> }` with a
//! synchronous reply, so a send only succeeds once the consumer has it.
//!
//! macOS only lets a process listen on a Mach service that launchd knows
//! about: the consumer must run from a LaunchAgent or LaunchDaemon plist
//! whose `MachServices` contains `<name>`. launchd then also starts the
//! consumer on demand when a producer connects. `samples/run_demo.sh` writes
//! and loads such a plist for the demo.

use std::ffi::{c_char, c_void, CStr, CString};
use std::io;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use block2::{Block, RcBlock};

use super::{uid_trusted, Incoming, Link, Server, Sink};
use crate::logging::{CONSUMER, PRODUCER};
use crate::{Error, Result, TrustedOwner};

type XpcObject = *mut c_void;

#[repr(C)]
struct Opaque {
    _private: [u8; 0],
}

extern "C" {
    static _xpc_type_dictionary: Opaque;
    static _xpc_type_error: Opaque;
    static _xpc_type_connection: Opaque;
    static _xpc_error_connection_invalid: Opaque;

    fn xpc_connection_create_mach_service(
        name: *const c_char,
        targetq: *mut c_void,
        flags: u64,
    ) -> XpcObject;
    fn xpc_connection_set_event_handler(connection: XpcObject, handler: &Block<dyn Fn(XpcObject)>);
    fn xpc_connection_resume(connection: XpcObject);
    fn xpc_connection_cancel(connection: XpcObject);
    fn xpc_connection_send_message(connection: XpcObject, message: XpcObject);
    fn xpc_connection_send_message_with_reply_sync(
        connection: XpcObject,
        message: XpcObject,
    ) -> XpcObject;
    fn xpc_connection_get_euid(connection: XpcObject) -> libc::uid_t;
    fn xpc_connection_get_pid(connection: XpcObject) -> libc::pid_t;
    fn xpc_dictionary_create(
        keys: *const *const c_char,
        values: *const XpcObject,
        count: usize,
    ) -> XpcObject;
    fn xpc_dictionary_create_reply(original: XpcObject) -> XpcObject;
    fn xpc_dictionary_set_data(
        xdict: XpcObject,
        key: *const c_char,
        bytes: *const c_void,
        length: usize,
    );
    fn xpc_dictionary_get_data(
        xdict: XpcObject,
        key: *const c_char,
        length: *mut usize,
    ) -> *const c_void;
    fn xpc_dictionary_set_bool(xdict: XpcObject, key: *const c_char, value: bool);
    fn xpc_dictionary_get_bool(xdict: XpcObject, key: *const c_char) -> bool;
    fn xpc_get_type(object: XpcObject) -> *const c_void;
    fn xpc_retain(object: XpcObject) -> XpcObject;
    fn xpc_release(object: XpcObject);
    fn xpc_copy_description(object: XpcObject) -> *mut c_char;
}

const XPC_CONNECTION_MACH_SERVICE_LISTENER: u64 = 1;
const KEY_EVENT: &CStr = c"event";
const KEY_HELLO: &CStr = c"hello";
const KEY_OK: &CStr = c"ok";

/// XPC objects are thread safe reference counted objects.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Obj(XpcObject);
unsafe impl Send for Obj {}
unsafe impl Sync for Obj {}

fn has_type(o: XpcObject, t: &'static Opaque) -> bool {
    // SAFETY: o is a live XPC object.
    unsafe { xpc_get_type(o) == (t as *const Opaque).cast() }
}

fn is_dictionary(o: XpcObject) -> bool {
    has_type(o, unsafe { &_xpc_type_dictionary })
}
fn is_error(o: XpcObject) -> bool {
    has_type(o, unsafe { &_xpc_type_error })
}
fn is_connection(o: XpcObject) -> bool {
    has_type(o, unsafe { &_xpc_type_connection })
}
fn is_connection_invalid(o: XpcObject) -> bool {
    o.cast_const() == unsafe { (&_xpc_error_connection_invalid as *const Opaque).cast::<c_void>() }
}

fn describe(o: XpcObject) -> String {
    // SAFETY: returns a malloc'd C string we free.
    unsafe {
        let p = xpc_copy_description(o);
        if p.is_null() {
            return "unknown XPC object".into();
        }
        let s = CStr::from_ptr(p).to_string_lossy().into_owned();
        libc::free(p.cast());
        s
    }
}

pub(super) fn endpoint(name: &str) -> String {
    format!("XPC service {name}")
}

fn c_name(name: &str) -> io::Result<CString> {
    CString::new(name).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains NUL"))
}

fn new_dictionary() -> XpcObject {
    // SAFETY: empty dictionary.
    unsafe { xpc_dictionary_create(std::ptr::null(), std::ptr::null(), 0) }
}

// ------------------------------------------------------------------ producer

struct XpcLink {
    conn: Obj,
    peer: String,
}

impl XpcLink {
    /// Sends `message` and waits for the consumer's reply.
    fn round_trip(&self, message: XpcObject) -> io::Result<()> {
        // SAFETY: live connection and message; both released here.
        unsafe {
            let reply = xpc_connection_send_message_with_reply_sync(self.conn.0, message);
            xpc_release(message);
            if is_error(reply) {
                let invalid = is_connection_invalid(reply);
                let why = describe(reply);
                xpc_release(reply);
                return Err(io::Error::new(
                    if invalid {
                        io::ErrorKind::NotFound
                    } else {
                        io::ErrorKind::BrokenPipe
                    },
                    why,
                ));
            }
            let ok = is_dictionary(reply) && xpc_dictionary_get_bool(reply, KEY_OK.as_ptr());
            xpc_release(reply);
            if ok {
                Ok(())
            } else {
                Err(io::Error::other("consumer did not acknowledge the message"))
            }
        }
    }
}

impl Link for XpcLink {
    fn send(&mut self, payload: &[u8]) -> io::Result<()> {
        let msg = new_dictionary();
        // SAFETY: payload outlives the call; XPC copies the bytes.
        unsafe {
            xpc_dictionary_set_data(
                msg,
                KEY_EVENT.as_ptr(),
                payload.as_ptr().cast(),
                payload.len(),
            )
        };
        self.round_trip(msg)
    }
    fn peer(&self) -> &str {
        &self.peer
    }
}

impl Drop for XpcLink {
    fn drop(&mut self) {
        // SAFETY: we own one reference.
        unsafe {
            xpc_connection_cancel(self.conn.0);
            xpc_release(self.conn.0);
        }
    }
}

pub(super) fn connect(name: &str, policy: &TrustedOwner) -> io::Result<Box<dyn Link>> {
    let cname = c_name(name)?;
    // SAFETY: NUL terminated name; default target queue.
    let conn =
        unsafe { xpc_connection_create_mach_service(cname.as_ptr(), std::ptr::null_mut(), 0) };
    if conn.is_null() {
        return Err(io::Error::other(
            "xpc_connection_create_mach_service returned NULL",
        ));
    }
    let service = name.to_owned();
    let handler: RcBlock<dyn Fn(XpcObject)> = RcBlock::new(move |event: XpcObject| {
        if is_error(event) {
            log::trace!(target: PRODUCER, "XPC connection to {service}: {}", describe(event));
        }
    });
    // SAFETY: XPC copies the block; the connection must have a handler before resume.
    unsafe {
        xpc_connection_set_event_handler(conn, &handler);
        xpc_connection_resume(conn);
    }
    let mut link = XpcLink {
        conn: Obj(conn),
        peer: String::new(),
    };
    // Hello round trip: finds a missing service now (and lets launchd start
    // an on-demand consumer), and makes the peer's identity available.
    let hello = new_dictionary();
    unsafe { xpc_dictionary_set_bool(hello, KEY_HELLO.as_ptr(), true) };
    link.round_trip(hello).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("XPC service {name} is not available (consumer not running or not registered with launchd)"),
            )
        } else {
            e
        }
    })?;
    // SAFETY: live connection that has exchanged a message.
    let (euid, pid) = unsafe { (xpc_connection_get_euid(conn), xpc_connection_get_pid(conn)) };
    if !uid_trusted(euid, policy) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("XPC service {name} is served by pid {pid} uid {euid}, not a trusted consumer ({policy:?})"),
        ));
    }
    link.peer = format!("consumer pid {pid} uid {euid}");
    Ok(Box::new(link))
}

// ------------------------------------------------------------------ consumer

struct XpcServer {
    name: String,
    listener: Option<Obj>,
    peers: Arc<Mutex<Vec<Obj>>>,
}

fn handle_peer(
    peer: XpcObject,
    policy: &TrustedOwner,
    max_bytes: usize,
    sink: &Sink,
    peers: &Arc<Mutex<Vec<Obj>>>,
) {
    // SAFETY: peer is a live connection handed to the listener's handler.
    let (euid, pid) = unsafe { (xpc_connection_get_euid(peer), xpc_connection_get_pid(peer)) };
    let who = format!("producer pid {pid} uid {euid}");
    if !uid_trusted(euid, policy) {
        log::warn!(target: CONSUMER, "rejected XPC connection from {who}: not {policy:?}");
        let noop: RcBlock<dyn Fn(XpcObject)> = RcBlock::new(|_: XpcObject| {});
        unsafe {
            xpc_connection_set_event_handler(peer, &noop);
            xpc_connection_resume(peer);
            xpc_connection_cancel(peer);
        }
        return;
    }
    log::debug!(target: CONSUMER, "accepted XPC connection from {who}");
    unsafe { xpc_retain(peer) };
    peers
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(Obj(peer));

    let (sink, peers, peer_obj) = (sink.clone(), Arc::clone(peers), Obj(peer));
    let handler: RcBlock<dyn Fn(XpcObject)> = RcBlock::new(move |msg: XpcObject| {
        if is_dictionary(msg) {
            let mut len = 0usize;
            // SAFETY: live dictionary; the returned pointer is valid while msg is.
            let data = unsafe { xpc_dictionary_get_data(msg, KEY_EVENT.as_ptr(), &mut len) };
            if !data.is_null() {
                if len > max_bytes {
                    log::warn!(target: CONSUMER, "{who} sent a {len} byte message (limit {max_bytes}); ignoring");
                } else {
                    let bytes =
                        unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) }.to_vec();
                    log::debug!(target: CONSUMER, "read {len} byte XPC message from {who}");
                    let _ = sink.send(Incoming::Payload {
                        peer: who.clone(),
                        bytes,
                    });
                }
            }
            // Acknowledge (also answers the producer's hello).
            unsafe {
                let reply = xpc_dictionary_create_reply(msg);
                if !reply.is_null() {
                    xpc_dictionary_set_bool(reply, KEY_OK.as_ptr(), true);
                    xpc_connection_send_message(peer_obj.0, reply);
                    xpc_release(reply);
                }
            }
        } else if is_error(msg) {
            log::debug!(target: CONSUMER, "{who} disconnected ({})", describe(msg));
            if is_connection_invalid(msg) {
                let mut list = peers.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(i) = list.iter().position(|p| *p == peer_obj) {
                    list.swap_remove(i);
                    unsafe { xpc_release(peer_obj.0) };
                }
            }
        }
    });
    unsafe {
        xpc_connection_set_event_handler(peer, &handler);
        xpc_connection_resume(peer);
    }
}

pub(super) fn serve(
    name: &str,
    policy: TrustedOwner,
    max_bytes: usize,
    sink: Sink,
) -> Result<Box<dyn Server>> {
    let cname = c_name(name)?;
    // SAFETY: NUL terminated name; default target queue.
    let listener = unsafe {
        xpc_connection_create_mach_service(
            cname.as_ptr(),
            std::ptr::null_mut(),
            XPC_CONNECTION_MACH_SERVICE_LISTENER,
        )
    };
    if listener.is_null() {
        return Err(Error::Ipc(format!("cannot create XPC listener for {name}")));
    }
    let peers: Arc<Mutex<Vec<Obj>>> = Arc::default();
    let (failed_tx, failed_rx) = mpsc::channel::<String>();
    let handler: RcBlock<dyn Fn(XpcObject)> = {
        let peers = Arc::clone(&peers);
        RcBlock::new(move |obj: XpcObject| {
            if is_connection(obj) {
                handle_peer(obj, &policy, max_bytes, &sink, &peers);
            } else if is_error(obj) {
                let why = describe(obj);
                log::debug!(target: CONSUMER, "XPC listener event: {why}");
                if is_connection_invalid(obj) {
                    let _ = failed_tx.send(why);
                }
            }
        })
    };
    unsafe {
        xpc_connection_set_event_handler(listener, &handler);
        xpc_connection_resume(listener);
    }
    // A failed launchd check-in is reported asynchronously, right away.
    if let Ok(why) = failed_rx.recv_timeout(Duration::from_millis(500)) {
        unsafe {
            xpc_connection_cancel(listener);
            xpc_release(listener);
        }
        return Err(Error::Ipc(format!(
            "cannot listen on XPC service {name} ({why}). macOS only lets a process listen on a \
             Mach service registered with launchd: run the consumer from a LaunchAgent or \
             LaunchDaemon plist whose MachServices contains {name} (samples/run_demo.sh shows how)"
        )));
    }
    Ok(Box::new(XpcServer {
        name: name.to_owned(),
        listener: Some(Obj(listener)),
        peers,
    }))
}

impl Server for XpcServer {
    fn stop(&mut self) {
        let Some(listener) = self.listener.take() else {
            return;
        };
        log::debug!(target: CONSUMER, "cancelling XPC listener {}", self.name);
        unsafe {
            xpc_connection_cancel(listener.0);
            for p in self
                .peers
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .drain(..)
            {
                xpc_connection_cancel(p.0);
                xpc_release(p.0);
            }
            xpc_release(listener.0);
        }
    }
}

impl Drop for XpcServer {
    fn drop(&mut self) {
        self.stop();
    }
}

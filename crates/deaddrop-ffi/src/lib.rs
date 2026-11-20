//! C ABI over `deaddrop`. The matching header is `include/deaddrop.h`.
//!
//! Conventions:
//! * Every fallible function returns an [`DdStatus`]. On failure a message
//!   is available from [`dd_last_error`] on the same thread.
//! * Strings are UTF-8 and NUL terminated.
//! * Handles are opaque; free them with the matching `*_free`.
//! * Panics never cross the boundary; they become `DD_ERR_PANIC`.

#![allow(clippy::missing_safety_doc)]

use std::cell::RefCell;
use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use deaddrop::{ConsumerConfig, DetectionEvent, Error, EventConsumer, EventProducer, TrustedOwner};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdStatus {
    Ok = 0,
    InvalidArgument = 1,
    Io = 2,
    InvalidEvent = 3,
    InsecureDirectory = 4,
    Watcher = 5,
    InvalidState = 6,
    Serialization = 7,
    Panic = 8,
    Ipc = 9,
}

/// Owner policy for `dd_consumer_init_ex`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdTrustedOwner {
    Privileged = 0,
    CurrentUser = 1,
    Any = 2,
}

/// Borrowed view of one event, valid only for the duration of the callback.
#[repr(C)]
pub struct DdEventView {
    pub schema_version: *const c_char,
    pub seq_id: u64,
    pub timestamp_utc: *const c_char,
    pub app_name: *const c_char,
    /// The metadata object as compact JSON, e.g. `{"confidence":98}`.
    pub metadata_json: *const c_char,
    /// The whole event as compact JSON.
    pub event_json: *const c_char,
}

pub type DdEventCallback =
    Option<unsafe extern "C" fn(event: *const DdEventView, user_data: *mut c_void)>;

pub struct DdProducer(EventProducer, CString);
pub struct DdConsumer(EventConsumer, CString);

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn set_last_error(msg: impl Into<Vec<u8>>) {
    let mut bytes = msg.into();
    bytes.retain(|&b| b != 0);
    let c = CString::new(bytes).unwrap_or_default();
    LAST_ERROR.with(|e| *e.borrow_mut() = c);
}

fn status_of(err: &Error) -> DdStatus {
    match err {
        Error::Io(_) => DdStatus::Io,
        Error::Serialization(_) => DdStatus::Serialization,
        Error::InvalidEvent(_) | Error::EventTooLarge { .. } => DdStatus::InvalidEvent,
        Error::InsecureDirectory { .. } => DdStatus::InsecureDirectory,
        Error::InvalidConfig(_) => DdStatus::InvalidArgument,
        Error::Watcher(_) => DdStatus::Watcher,
        Error::Ipc(_) => DdStatus::Ipc,
        Error::AlreadyWatching | Error::ShutDown => DdStatus::InvalidState,
        _ => DdStatus::Io,
    }
}

struct Fail(DdStatus, String);

impl From<Error> for Fail {
    fn from(e: Error) -> Self {
        Fail(status_of(&e), e.to_string())
    }
}

fn bad_arg(msg: &str) -> Fail {
    Fail(DdStatus::InvalidArgument, msg.to_owned())
}

/// Runs `f`, converting errors and panics into a status code.
fn guard(f: impl FnOnce() -> Result<(), Fail>) -> DdStatus {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => {
            set_last_error("");
            DdStatus::Ok
        }
        Ok(Err(Fail(status, msg))) => {
            set_last_error(msg);
            status
        }
        Err(_) => {
            set_last_error("internal panic in deaddrop");
            DdStatus::Panic
        }
    }
}

unsafe fn str_arg<'a>(p: *const c_char, what: &str) -> Result<&'a str, Fail> {
    if p.is_null() {
        return Err(bad_arg(&format!("{what} is NULL")));
    }
    CStr::from_ptr(p)
        .to_str()
        .map_err(|_| bad_arg(&format!("{what} is not valid UTF-8")))
}

unsafe fn bytes_arg<'a>(p: *const c_char, len: usize, what: &str) -> Result<&'a str, Fail> {
    if p.is_null() {
        return Err(bad_arg(&format!("{what} is NULL")));
    }
    std::str::from_utf8(std::slice::from_raw_parts(p.cast::<u8>(), len))
        .map_err(|_| bad_arg(&format!("{what} is not valid UTF-8")))
}

unsafe fn out_ptr<T>(out: *mut *mut T) -> Result<&'static mut *mut T, Fail> {
    if out.is_null() {
        return Err(bad_arg("out pointer is NULL"));
    }
    *out = ptr::null_mut();
    Ok(&mut *out)
}

/// Library version, static storage.
#[no_mangle]
pub extern "C" fn dd_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

/// Message for the last failed call on this thread ("" after success).
/// Valid until the next `dd_*` call on this thread.
#[no_mangle]
pub extern "C" fn dd_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}

// ---------------------------------------------------------------- logging

/// Installs the library's stderr logger once, at the level in DEADDROP_LOG.
fn ensure_logging() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        deaddrop::logging::init_from_env();
    });
}

/// Sets the diagnostic log level: "off", "error", "warn" (default), "info",
/// "debug" or "trace". Overrides DEADDROP_LOG. Log lines go to stderr and
/// start with "[deaddrop:producer]" or "[deaddrop:consumer]".
#[no_mangle]
pub unsafe extern "C" fn dd_set_log_level(level: *const c_char) -> DdStatus {
    guard(|| {
        let raw = str_arg(level, "level")?;
        let level = deaddrop::logging::parse_level(raw).ok_or_else(|| {
            bad_arg(&format!(
                "unknown log level {raw:?}; use off, error, warn, info, debug or trace"
            ))
        })?;
        ensure_logging();
        deaddrop::logging::init_with_level(level);
        Ok(())
    })
}

// ---------------------------------------------------------------- producer

#[no_mangle]
pub unsafe extern "C" fn dd_producer_init(
    dir: *const c_char,
    out: *mut *mut DdProducer,
) -> DdStatus {
    guard(|| {
        ensure_logging();
        let out = out_ptr(out)?;
        let dir = str_arg(dir, "dir")?;
        let p = EventProducer::init(dir)?;
        let endpoint = lossy_cstring(&p.endpoint());
        *out = Box::into_raw(Box::new(DdProducer(p, endpoint)));
        Ok(())
    })
}

/// Publishes a full event given as JSON (same schema the consumer receives).
#[no_mangle]
pub unsafe extern "C" fn dd_producer_publish_json(
    producer: *const DdProducer,
    json: *const c_char,
    len: usize,
) -> DdStatus {
    guard(|| {
        let p = producer
            .as_ref()
            .ok_or_else(|| bad_arg("producer is NULL"))?;
        let json = bytes_arg(json, len, "json")?;
        let event = DetectionEvent::from_json(json)?;
        p.0.publish_event(&event)?;
        Ok(())
    })
}

/// Publishes an event stamped with the current time and schema version.
/// `metadata_json` may be NULL or a JSON object.
#[no_mangle]
pub unsafe extern "C" fn dd_producer_publish(
    producer: *const DdProducer,
    seq_id: u64,
    app_name: *const c_char,
    metadata_json: *const c_char,
) -> DdStatus {
    guard(|| {
        let p = producer
            .as_ref()
            .ok_or_else(|| bad_arg("producer is NULL"))?;
        let mut event = DetectionEvent::new(seq_id, str_arg(app_name, "app_name")?);
        if !metadata_json.is_null() {
            let raw = str_arg(metadata_json, "metadata_json")?;
            match serde_json::from_str(raw).map_err(Error::from)? {
                serde_json::Value::Object(map) => event.metadata = map,
                _ => return Err(bad_arg("metadata_json must be a JSON object")),
            }
        }
        p.0.publish_event(&event)?;
        Ok(())
    })
}

/// Transport in use, e.g. "file: /path" or "ipc: named pipe \\.\pipe\name".
/// Owned by the producer; valid until dd_producer_free. NULL if producer is NULL.
#[no_mangle]
pub unsafe extern "C" fn dd_producer_endpoint(producer: *const DdProducer) -> *const c_char {
    producer.as_ref().map_or(ptr::null(), |p| p.1.as_ptr())
}

#[no_mangle]
pub unsafe extern "C" fn dd_producer_shutdown(producer: *const DdProducer) -> DdStatus {
    guard(|| {
        let p = producer
            .as_ref()
            .ok_or_else(|| bad_arg("producer is NULL"))?;
        p.0.shutdown()?;
        Ok(())
    })
}

/// Shuts down (if needed) and frees. NULL is a no-op.
#[no_mangle]
pub unsafe extern "C" fn dd_producer_free(producer: *mut DdProducer) {
    if !producer.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(Box::from_raw(producer))));
    }
}

// ---------------------------------------------------------------- consumer

/// Consumer with defaults: directory must be owned by root / SYSTEM / Administrators.
#[no_mangle]
pub unsafe extern "C" fn dd_consumer_init(
    dir: *const c_char,
    out: *mut *mut DdConsumer,
) -> DdStatus {
    dd_consumer_init_ex(dir, DdTrustedOwner::Privileged as u32, 1, 5000, out)
}

/// `rescan_interval_ms` = 0 disables the safety net rescan.
/// `trusted_owner` is an `dd_trusted_owner` value; taken as an integer so an
/// out of range value from C is an error rather than undefined behavior.
#[no_mangle]
pub unsafe extern "C" fn dd_consumer_init_ex(
    dir: *const c_char,
    trusted_owner: u32,
    replay_existing: i32,
    rescan_interval_ms: u32,
    out: *mut *mut DdConsumer,
) -> DdStatus {
    guard(|| {
        ensure_logging();
        let out = out_ptr(out)?;
        let dir = str_arg(dir, "dir")?;
        let policy = match trusted_owner {
            0 => TrustedOwner::Privileged,
            1 => TrustedOwner::CurrentUser,
            2 => TrustedOwner::Any,
            other => return Err(bad_arg(&format!("unknown trusted_owner {other}"))),
        };
        let c = EventConsumer::init_with(dir, |cfg: &mut ConsumerConfig| {
            cfg.trusted_owner = policy;
            cfg.replay_existing = replay_existing != 0;
            cfg.rescan_interval = (rescan_interval_ms > 0)
                .then(|| std::time::Duration::from_millis(rescan_interval_ms.into()));
        })?;
        let endpoint = lossy_cstring(&c.endpoint());
        *out = Box::into_raw(Box::new(DdConsumer(c, endpoint)));
        Ok(())
    })
}

struct UserData(*mut c_void);
// SAFETY: the header documents that user_data is used from the watcher
// thread; the caller is responsible for its thread safety.
unsafe impl Send for UserData {}
unsafe impl Sync for UserData {}

fn lossy_cstring(s: &str) -> CString {
    CString::new(s.replace('\0', "\u{FFFD}")).expect("NULs removed")
}

/// Registers `callback`; it runs on the watcher thread with `user_data`.
#[no_mangle]
pub unsafe extern "C" fn dd_consumer_register_listener(
    consumer: *mut DdConsumer,
    callback: DdEventCallback,
    user_data: *mut c_void,
) -> DdStatus {
    guard(|| {
        let c = consumer
            .as_mut()
            .ok_or_else(|| bad_arg("consumer is NULL"))?;
        let cb = callback.ok_or_else(|| bad_arg("callback is NULL"))?;
        let ud = UserData(user_data);
        c.0.register_event_listener(move |event: DetectionEvent| {
            let schema = lossy_cstring(&event.schema_version);
            let ts = lossy_cstring(&event.timestamp_utc);
            let app = lossy_cstring(&event.app_name);
            let meta = lossy_cstring(&serde_json::to_string(&event.metadata).unwrap_or_default());
            let full = lossy_cstring(&event.to_json().unwrap_or_default());
            let view = DdEventView {
                schema_version: schema.as_ptr(),
                seq_id: event.seq_id,
                timestamp_utc: ts.as_ptr(),
                app_name: app.as_ptr(),
                metadata_json: meta.as_ptr(),
                event_json: full.as_ptr(),
            };
            let ud = &ud;
            // SAFETY: contract of the registered callback.
            unsafe { cb(&view, ud.0) };
        })?;
        Ok(())
    })
}

/// Transport in use. Owned by the consumer; valid until dd_consumer_free.
#[no_mangle]
pub unsafe extern "C" fn dd_consumer_endpoint(consumer: *const DdConsumer) -> *const c_char {
    consumer.as_ref().map_or(ptr::null(), |c| c.1.as_ptr())
}

#[no_mangle]
pub unsafe extern "C" fn dd_consumer_start(consumer: *mut DdConsumer) -> DdStatus {
    guard(|| {
        let c = consumer
            .as_mut()
            .ok_or_else(|| bad_arg("consumer is NULL"))?;
        c.0.start_watching()?;
        Ok(())
    })
}

/// Stops the watcher and joins its thread. After it returns no callback is
/// running or will run, so `user_data` may be released.
#[no_mangle]
pub unsafe extern "C" fn dd_consumer_shutdown(consumer: *mut DdConsumer) -> DdStatus {
    guard(|| {
        let c = consumer
            .as_mut()
            .ok_or_else(|| bad_arg("consumer is NULL"))?;
        c.0.shutdown()?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn dd_consumer_free(consumer: *mut DdConsumer) {
    if !consumer.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(Box::from_raw(consumer))));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    unsafe extern "C" fn on_event(ev: *const DdEventView, ud: *mut c_void) {
        let tx = &*(ud as *const mpsc::Sender<(u64, String, String)>);
        let ev = &*ev;
        let app = CStr::from_ptr(ev.app_name).to_string_lossy().into_owned();
        let meta = CStr::from_ptr(ev.metadata_json)
            .to_string_lossy()
            .into_owned();
        let _ = tx.send((ev.seq_id, app, meta));
    }

    #[test]
    fn c_abi_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = CString::new(tmp.path().join("ev").to_str().unwrap()).unwrap();
        unsafe {
            let mut p = ptr::null_mut();
            assert_eq!(dd_producer_init(dir.as_ptr(), &mut p), DdStatus::Ok);

            let mut c = ptr::null_mut();
            assert_eq!(
                dd_consumer_init_ex(
                    dir.as_ptr(),
                    DdTrustedOwner::CurrentUser as u32,
                    1,
                    0,
                    &mut c
                ),
                DdStatus::Ok
            );
            let (tx, rx) = mpsc::channel::<(u64, String, String)>();
            let tx = Box::new(tx);
            let ud = &*tx as *const _ as *mut c_void;
            assert_eq!(
                dd_consumer_register_listener(c, Some(on_event), ud),
                DdStatus::Ok
            );
            assert_eq!(dd_consumer_start(c), DdStatus::Ok);

            let app = CString::new("TargetApp").unwrap();
            let meta = CString::new(r#"{"confidence":98}"#).unwrap();
            assert_eq!(
                dd_producer_publish(p, 5, app.as_ptr(), meta.as_ptr()),
                DdStatus::Ok
            );
            let json = r#"{"schema_version":"1.0","seq_id":6,"timestamp_utc":"2026-11-18T17:45:00Z","app_name":"B"}"#;
            assert_eq!(
                dd_producer_publish_json(p, json.as_ptr().cast(), json.len()),
                DdStatus::Ok
            );

            let t = std::time::Duration::from_secs(5);
            assert_eq!(
                rx.recv_timeout(t).unwrap(),
                (5, "TargetApp".into(), r#"{"confidence":98}"#.into())
            );
            assert_eq!(rx.recv_timeout(t).unwrap(), (6, "B".into(), "{}".into()));

            let bad = CString::new("[1]").unwrap();
            assert_eq!(
                dd_producer_publish(p, 7, app.as_ptr(), bad.as_ptr()),
                DdStatus::InvalidArgument
            );
            assert!(!CStr::from_ptr(dd_last_error()).to_bytes().is_empty());
            assert_eq!(
                dd_producer_publish(p, 8, ptr::null(), ptr::null()),
                DdStatus::InvalidArgument
            );

            assert_eq!(dd_consumer_shutdown(c), DdStatus::Ok);
            dd_consumer_free(c);
            assert_eq!(dd_producer_shutdown(p), DdStatus::Ok);
            assert_eq!(
                dd_producer_publish(p, 9, app.as_ptr(), ptr::null()),
                DdStatus::InvalidState
            );
            dd_producer_free(p);
            drop(tx);
        }
    }
}

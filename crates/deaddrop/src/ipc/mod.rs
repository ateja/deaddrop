//! IPC transport: events go straight from producer to consumer over the OS
//! IPC channel, nothing is written to disk.
//!
//! | OS | Channel | Endpoint for `ipc.name: com.example.events` |
//! |---|---|---|
//! | Windows | named pipe (byte mode, local clients only) | `\\.\pipe\com.example.events` |
//! | macOS | XPC Mach service | `com.example.events` |
//! | Linux and other Unix | Unix domain socket | `/tmp/com.example.events.sock` |
//!
//! The consumer listens and the producer connects, so on macOS launchd can
//! start the consumer on demand. Each side checks the OS identity of the
//! process on the other end against a [`TrustedOwner`] policy before any
//! event crosses.
//!
//! The producer never blocks on the consumer: `publish_event` queues the
//! event in memory (bounded by `max_pending`, oldest dropped first) and a
//! delivery thread sends it, connecting and reconnecting as needed.
//! `shutdown()` waits up to `flush_timeout` for the queue to drain. Unlike
//! the file transport, events still queued when the producer process dies
//! are lost.

use std::collections::VecDeque;
use std::io;
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::config::IpcSettings;
use crate::logging::{CONSUMER, PRODUCER};
use crate::transport::{ConsumerTransport, Dispatcher, ProducerTransport};
use crate::{DetectionEvent, Error, Result, TrustedOwner, DEFAULT_MAX_EVENT_BYTES};

#[cfg(windows)]
mod named_pipe;
#[cfg(any(windows, all(unix, not(target_os = "macos"))))]
mod stream;
#[cfg(all(unix, not(target_os = "macos")))]
mod unix_socket;
#[cfg(target_os = "macos")]
mod xpc;

#[cfg(windows)]
use named_pipe as platform;
#[cfg(all(unix, not(target_os = "macos")))]
use unix_socket as platform;
#[cfg(target_os = "macos")]
use xpc as platform;

#[cfg(unix)]
pub(crate) fn euid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

/// Unix peer check shared by Unix sockets and XPC. `CurrentUser` also
/// accepts root, mirroring the file transport's directory owner check.
#[cfg(unix)]
pub(crate) fn uid_trusted(uid: u32, policy: &TrustedOwner) -> bool {
    match policy {
        TrustedOwner::Privileged => uid == 0,
        TrustedOwner::CurrentUser => uid == euid() || uid == 0,
        TrustedOwner::Uid(expected) => uid == *expected,
        TrustedOwner::Any => true,
    }
}

/// Human readable endpoint for `name` on this OS, for logs and errors.
pub(crate) fn describe_endpoint(name: &str) -> String {
    platform::endpoint(name)
}

/// One message from a platform server to the consumer's dispatcher thread.
pub(crate) enum Incoming {
    Payload { peer: String, bytes: Vec<u8> },
    Stop,
}

pub(crate) type Sink = mpsc::Sender<Incoming>;

/// A producer side connection that delivers one serialized event at a time.
pub(crate) trait Link: Send {
    fn send(&mut self, payload: &[u8]) -> io::Result<()>;
    /// Who is on the other end, for logs.
    fn peer(&self) -> &str;
}

/// A running consumer side listener.
pub(crate) trait Server: Send {
    /// Stops accepting, closes every connection and joins its threads.
    fn stop(&mut self);
}

/// Opens a link to the consumer's endpoint and verifies its identity.
fn connect(name: &str, policy: &TrustedOwner) -> io::Result<Box<dyn Link>> {
    platform::connect(name, policy)
}

fn serve(
    name: &str,
    policy: TrustedOwner,
    max_bytes: usize,
    sink: Sink,
) -> Result<Box<dyn Server>> {
    platform::serve(name, policy, max_bytes, sink)
}

// ------------------------------------------------------------------ producer

struct Pending {
    id: u64,
    seq: u64,
    bytes: Arc<Vec<u8>>,
    queued_at: Instant,
}

#[derive(Default)]
struct Queue {
    items: VecDeque<Pending>,
    next_id: u64,
    closing_deadline: Option<Instant>,
    dropped: u64,
}

struct Shared {
    queue: Mutex<Queue>,
    wake: Condvar,
}

/// Producer side of the IPC transport.
pub struct IpcProducer {
    settings: IpcSettings,
    endpoint: String,
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

const MIN_BACKOFF: Duration = Duration::from_millis(50);
const MAX_BACKOFF: Duration = Duration::from_secs(1);

impl IpcProducer {
    pub(crate) fn new(settings: IpcSettings) -> Result<Self> {
        let endpoint = describe_endpoint(&settings.name);
        let policy = settings
            .trusted_peer
            .clone()
            .unwrap_or(TrustedOwner::Privileged);
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue::default()),
            wake: Condvar::new(),
        });
        log::debug!(
            target: PRODUCER,
            "IPC transport: will deliver to {endpoint} (consumer must be {policy:?}; queue up to {} events while it is unreachable)",
            settings.max_pending
        );
        let worker = {
            let shared = Arc::clone(&shared);
            let name = settings.name.clone();
            let endpoint = endpoint.clone();
            thread::Builder::new()
                .name("deaddrop-ipc-producer".into())
                .spawn(move || deliver_loop(&shared, &name, &endpoint, &policy))?
        };
        Ok(Self {
            settings,
            endpoint,
            shared,
            worker: Mutex::new(Some(worker)),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.shared.queue.lock().unwrap_or_else(|p| p.into_inner())
    }
}

fn deliver_loop(shared: &Shared, name: &str, endpoint: &str, policy: &TrustedOwner) {
    let mut link: Option<Box<dyn Link>> = None;
    let mut backoff = MIN_BACKOFF;
    let mut unreachable_since: Option<Instant> = None;
    loop {
        // Wait for the next event, or exit once closing and drained.
        let (id, seq, bytes, queued_at) = {
            let mut q = shared.queue.lock().unwrap_or_else(|p| p.into_inner());
            loop {
                if let Some(deadline) = q.closing_deadline {
                    if q.items.is_empty() {
                        return;
                    }
                    if Instant::now() >= deadline {
                        let n = q.items.len();
                        q.items.clear();
                        log::warn!(
                            target: PRODUCER,
                            "shutdown: dropping {n} undelivered event(s); {endpoint} was not reachable in time"
                        );
                        return;
                    }
                }
                if let Some(p) = q.items.front() {
                    break (p.id, p.seq, Arc::clone(&p.bytes), p.queued_at);
                }
                q = shared.wake.wait(q).unwrap_or_else(|p| p.into_inner());
            }
        };

        if link.is_none() {
            match connect(name, policy) {
                Ok(l) => {
                    match unreachable_since.take() {
                        Some(t) => log::debug!(
                            target: PRODUCER,
                            "connected to {endpoint} ({}) after {:.0} ms",
                            l.peer(),
                            t.elapsed().as_secs_f64() * 1e3
                        ),
                        None => {
                            log::debug!(target: PRODUCER, "connected to {endpoint} ({})", l.peer())
                        }
                    }
                    backoff = MIN_BACKOFF;
                    link = Some(l);
                }
                Err(e) => {
                    if unreachable_since.is_none() {
                        unreachable_since = Some(Instant::now());
                        let pending = shared.queue.lock().map(|q| q.items.len()).unwrap_or(0);
                        log::debug!(
                            target: PRODUCER,
                            "consumer not reachable at {endpoint} ({e}); holding {pending} event(s) in memory and retrying"
                        );
                        if e.kind() == io::ErrorKind::PermissionDenied {
                            log::warn!(target: PRODUCER, "refusing to send to {endpoint}: {e}");
                        }
                    }
                    let q = shared.queue.lock().unwrap_or_else(|p| p.into_inner());
                    let _ = shared.wake.wait_timeout(q, backoff);
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
            }
        }

        let l = link.as_mut().expect("connected above");
        let started = Instant::now();
        match l.send(&bytes) {
            Ok(()) => {
                log::debug!(
                    target: PRODUCER,
                    "seq={seq} delivered {} bytes to {endpoint} in {:.2} ms (queued {:.2} ms before that)",
                    bytes.len(),
                    started.elapsed().as_secs_f64() * 1e3,
                    started.duration_since(queued_at).as_secs_f64() * 1e3
                );
                let mut q = shared.queue.lock().unwrap_or_else(|p| p.into_inner());
                if q.items.front().is_some_and(|p| p.id == id) {
                    q.items.pop_front();
                }
            }
            Err(e) => {
                log::debug!(
                    target: PRODUCER,
                    "seq={seq} send to {endpoint} failed ({e}); reconnecting, event stays queued"
                );
                link = None;
                unreachable_since.get_or_insert_with(Instant::now);
            }
        }
    }
}

impl ProducerTransport for IpcProducer {
    fn publish(&self, event: &DetectionEvent) -> Result<()> {
        let bytes = serde_json::to_vec(event)?;
        if bytes.len() > DEFAULT_MAX_EVENT_BYTES {
            return Err(Error::EventTooLarge {
                size: bytes.len(),
                max: DEFAULT_MAX_EVENT_BYTES,
            });
        }
        let size = bytes.len();
        let mut q = self.lock();
        if q.closing_deadline.is_some() {
            return Err(Error::ShutDown);
        }
        if q.items.len() >= self.settings.max_pending {
            if let Some(old) = q.items.pop_front() {
                q.dropped += 1;
                log::warn!(
                    target: PRODUCER,
                    "queue full ({} events): dropped oldest undelivered event seq={}",
                    self.settings.max_pending,
                    old.seq
                );
            }
        }
        let id = q.next_id;
        q.next_id += 1;
        q.items.push_back(Pending {
            id,
            seq: event.seq_id,
            bytes: Arc::new(bytes),
            queued_at: Instant::now(),
        });
        let pending = q.items.len();
        drop(q);
        self.shared.wake.notify_all();
        log::debug!(
            target: PRODUCER,
            "seq={} queued {size} bytes for {} ({pending} pending)",
            event.seq_id,
            self.endpoint
        );
        Ok(())
    }

    fn shutdown(&self) -> Result<()> {
        {
            let mut q = self.lock();
            if q.closing_deadline.is_none() {
                q.closing_deadline = Some(Instant::now() + self.settings.flush_timeout);
                if !q.items.is_empty() {
                    log::debug!(
                        target: PRODUCER,
                        "shutdown: delivering {} queued event(s), waiting up to {} ms",
                        q.items.len(),
                        self.settings.flush_timeout.as_millis()
                    );
                }
            }
        }
        self.shared.wake.notify_all();
        let worker = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(w) = worker {
            let _ = w.join();
        }
        Ok(())
    }

    fn describe(&self) -> String {
        format!("ipc: {}", self.endpoint)
    }
}

impl Drop for IpcProducer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

// ------------------------------------------------------------------ consumer

struct Running {
    server: Box<dyn Server>,
    tx: Sink,
    worker: JoinHandle<()>,
}

/// Consumer side of the IPC transport.
pub struct IpcConsumer {
    name: String,
    endpoint: String,
    policy: TrustedOwner,
    max_bytes: usize,
    running: Option<Running>,
}

impl IpcConsumer {
    pub(crate) fn new(settings: &IpcSettings, policy: TrustedOwner, max_bytes: usize) -> Self {
        Self {
            name: settings.name.clone(),
            endpoint: describe_endpoint(&settings.name),
            policy,
            max_bytes,
            running: None,
        }
    }
}

fn dispatch_loop(rx: mpsc::Receiver<Incoming>, dispatcher: Dispatcher) {
    for msg in rx {
        let (peer, bytes) = match msg {
            Incoming::Stop => break,
            Incoming::Payload { peer, bytes } => (peer, bytes),
        };
        let event = match serde_json::from_slice::<DetectionEvent>(&bytes)
            .map_err(Error::from)
            .and_then(|e| e.validate().map(|()| e))
        {
            Ok(e) => e,
            Err(e) => {
                log::warn!(target: CONSUMER, "rejected {} byte message from {peer}: {e}", bytes.len());
                continue;
            }
        };
        log::debug!(
            target: CONSUMER,
            "received event seq={} app={:?} ({} bytes) from {peer}",
            event.seq_id,
            event.app_name,
            bytes.len()
        );
        log::debug!(
            target: CONSUMER,
            "delivering seq={} to {} listener(s)",
            event.seq_id,
            dispatcher.listener_count()
        );
        dispatcher.dispatch(event);
    }
}

impl ConsumerTransport for IpcConsumer {
    fn start(&mut self, dispatcher: Dispatcher) -> Result<()> {
        if self.running.is_some() {
            return Err(Error::AlreadyWatching);
        }
        let (tx, rx) = mpsc::channel();
        let server = serve(&self.name, self.policy.clone(), self.max_bytes, tx.clone())?;
        log::debug!(
            target: CONSUMER,
            "IPC transport: listening on {} (producers must be {:?})",
            self.endpoint,
            self.policy
        );
        let worker = thread::Builder::new()
            .name("deaddrop-ipc-consumer".into())
            .spawn(move || dispatch_loop(rx, dispatcher))?;
        self.running = Some(Running { server, tx, worker });
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        let Some(Running {
            mut server,
            tx,
            worker,
        }) = self.running.take()
        else {
            return Ok(());
        };
        log::debug!(target: CONSUMER, "stopping: closing {} and joining worker thread", self.endpoint);
        server.stop();
        let _ = tx.send(Incoming::Stop);
        if worker.thread().id() == thread::current().id() {
            return Ok(());
        }
        worker
            .join()
            .map_err(|_| Error::Ipc("consumer worker thread panicked".into()))
    }

    fn describe(&self) -> String {
        format!("ipc: {}", self.endpoint)
    }
}

impl Drop for IpcConsumer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

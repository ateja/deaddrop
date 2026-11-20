use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::io::{self, Read};
use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use notify::event::{EventKind, ModifyKind};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};

use super::{naming, security};
use crate::logging::CONSUMER as LOG;
use crate::transport::{ConsumerTransport, Dispatcher};
use crate::{ConsumerConfig, DetectionEvent, Error, Result};

/// Transient read failures (for example a Windows sharing violation caused
/// by an antivirus scan) are retried this many times before giving up.
const MAX_READ_ATTEMPTS: u8 = 5;
/// Wait used when `rescan_interval` is `None`; only affects how often the
/// worker checks for shutdown if the channel stays silent.
const IDLE_WAIT: Duration = Duration::from_secs(3600);

enum Signal {
    Wake,
    Stop,
}

/// Why a directory scan ran, for the logs.
#[derive(Clone, Copy)]
enum ScanReason {
    Startup,
    Notification,
    PeriodicRescan,
}

impl ScanReason {
    fn as_str(self) -> &'static str {
        match self {
            ScanReason::Startup => "startup scan",
            ScanReason::Notification => "scan after OS notification",
            ScanReason::PeriodicRescan => "periodic safety-net rescan",
        }
    }
}

/// Logs one watcher event. Creates, renames and deletes are the interesting
/// ones (debug); data writes, metadata and access events are trace only.
fn log_os_event(event: &notify::Event) {
    let interesting = matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(_))
    );
    let level = if interesting {
        log::Level::Debug
    } else {
        log::Level::Trace
    };
    if !log::log_enabled!(target: LOG, level) {
        return;
    }
    let names: Vec<String> = event
        .paths
        .iter()
        .map(|p| {
            p.file_name().map_or_else(
                || p.display().to_string(),
                |n| n.to_string_lossy().into_owned(),
            )
        })
        .collect();
    log::log!(target: LOG, level, "OS notification {:?}: {}", event.kind, names.join(" -> "));
}

struct Running {
    watcher: RecommendedWatcher,
    tx: mpsc::Sender<Signal>,
    worker: JoinHandle<()>,
}

/// Watches the shared directory and dispatches complete event files.
///
/// The OS notification is only a wakeup. The directory listing is the
/// source of truth, so coalesced, reordered or dropped notifications cannot
/// cause a missed event (the periodic rescan covers the dropped case).
pub struct FileConsumer {
    config: ConsumerConfig,
    running: Option<Running>,
}

impl FileConsumer {
    pub fn new(config: ConsumerConfig) -> Self {
        Self {
            config,
            running: None,
        }
    }
}

impl ConsumerTransport for FileConsumer {
    fn start(&mut self, dispatcher: Dispatcher) -> Result<()> {
        if self.running.is_some() {
            return Err(Error::AlreadyWatching);
        }
        security::verify_consumer_dir(&self.config.dir, &self.config.trusted_owner)?;
        log::debug!(
            target: LOG,
            "shared directory {} passed trust checks (owner policy {:?})",
            self.config.dir.display(),
            self.config.trusted_owner
        );

        let (tx, rx) = mpsc::channel();
        let wake = tx.clone();
        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                match &res {
                    Ok(event) => log_os_event(event),
                    Err(e) => log::warn!(target: LOG, "watcher reported {e}; rescanning"),
                }
                let _ = wake.send(Signal::Wake);
            })?;
        // Register the watch before the initial scan: anything renamed in
        // after this point triggers a wakeup, anything before it is listed.
        watcher.watch(&self.config.dir, RecursiveMode::NonRecursive)?;
        log::debug!(
            target: LOG,
            "watching {} with the OS file watcher ({:?}); safety-net rescan every {}",
            self.config.dir.display(),
            <RecommendedWatcher as Watcher>::kind(),
            self.config
                .rescan_interval
                .map_or_else(|| "never".to_owned(), |d| format!("{:.1} s", d.as_secs_f64()))
        );

        let mut scanner = Scanner::new(self.config.clone());
        let interval = self.config.rescan_interval.unwrap_or(IDLE_WAIT);
        let worker = thread::Builder::new()
            .name("deaddrop-consumer".into())
            .spawn(move || {
                scanner.initial(&dispatcher);
                loop {
                    match rx.recv_timeout(interval) {
                        Ok(Signal::Stop) | Err(RecvTimeoutError::Disconnected) => break,
                        Ok(Signal::Wake) => {
                            // Coalesce a burst of notifications into one scan.
                            let mut stop = false;
                            while let Ok(s) = rx.try_recv() {
                                if matches!(s, Signal::Stop) {
                                    stop = true;
                                    break;
                                }
                            }
                            if stop {
                                break;
                            }
                            scanner.scan(&dispatcher, ScanReason::Notification);
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            scanner.scan(&dispatcher, ScanReason::PeriodicRescan)
                        }
                    }
                }
            })?;

        self.running = Some(Running {
            watcher,
            tx,
            worker,
        });
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        let Some(Running {
            watcher,
            tx,
            worker,
        }) = self.running.take()
        else {
            return Ok(());
        };
        drop(watcher); // unregister from the OS first: no new wakeups
        let _ = tx.send(Signal::Stop);
        log::debug!(target: LOG, "stopping: OS watcher released, joining worker thread");
        if worker.thread().id() == thread::current().id() {
            // Called from inside a listener: the loop exits after this batch.
            return Ok(());
        }
        worker
            .join()
            .map_err(|_| Error::Watcher("consumer worker thread panicked".into()))
    }
}

impl Drop for FileConsumer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

struct Scanner {
    config: ConsumerConfig,
    /// Files already dispatched (or permanently rejected) and still on disk.
    seen: HashSet<OsString>,
    failures: HashMap<OsString, u8>,
    /// In-flight temp files already reported, so each is logged once.
    announced_temps: HashSet<OsString>,
}

impl Scanner {
    fn new(config: ConsumerConfig) -> Self {
        Self {
            config,
            seen: HashSet::new(),
            failures: HashMap::new(),
            announced_temps: HashSet::new(),
        }
    }

    fn initial(&mut self, dispatcher: &Dispatcher) {
        if self.config.replay_existing {
            self.scan(dispatcher, ScanReason::Startup);
        } else if let Ok(entries) = fs::read_dir(&self.config.dir) {
            self.seen.extend(
                entries
                    .flatten()
                    .map(|e| e.file_name())
                    .filter(|n| n.to_str().and_then(naming::parse_event_file_name).is_some()),
            );
            log::debug!(
                target: LOG,
                "startup: skipping {} event file(s) already on disk (replay_existing=false)",
                self.seen.len()
            );
        }
    }

    fn scan(&mut self, dispatcher: &Dispatcher, reason: ScanReason) {
        let entries = match fs::read_dir(&self.config.dir) {
            Ok(e) => e,
            Err(e) => {
                log::warn!(target: LOG, "cannot list {}: {e}", self.config.dir.display());
                return;
            }
        };

        let mut present = HashSet::new();
        let mut batch: Vec<(u64, OsString, DetectionEvent)> = Vec::new();
        let mut in_flight = HashSet::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(seq_id) = name.to_str().and_then(naming::parse_event_file_name) else {
                if name.to_str().is_some_and(naming::is_temp_file_name) {
                    in_flight.insert(name);
                }
                continue;
            };
            present.insert(name.clone());
            if self.seen.contains(&name) {
                continue;
            }
            log::debug!(target: LOG, "{}: reading new event file {}", reason.as_str(), entry.path().display());
            match read_event(&entry.path(), seq_id, self.config.max_event_bytes) {
                Ok(event) => {
                    log::debug!(
                        target: LOG,
                        "received event seq={} app={:?} from {}",
                        event.seq_id,
                        event.app_name,
                        name.to_string_lossy()
                    );
                    self.seen.insert(name.clone());
                    self.failures.remove(&name);
                    batch.push((seq_id, name, event));
                }
                Err(ReadError::Gone) => {
                    present.remove(&name);
                }
                Err(ReadError::Rejected(why)) => {
                    log::warn!(target: LOG, "skipping {}: {why}", entry.path().display());
                    self.seen.insert(name);
                }
                Err(ReadError::Transient(e)) => {
                    let n = self.failures.entry(name.clone()).or_insert(0);
                    *n += 1;
                    if *n >= MAX_READ_ATTEMPTS {
                        log::warn!(
                            target: LOG,
                            "giving up on {} after {n} attempts: {e}",
                            entry.path().display()
                        );
                        self.failures.remove(&name);
                        self.seen.insert(name);
                    }
                }
            }
        }

        for tmp in &in_flight {
            if self.announced_temps.insert(tmp.clone()) {
                log::debug!(
                    target: LOG,
                    "{}: ignoring {} (producer is still writing it; it becomes visible only after the atomic rename)",
                    reason.as_str(),
                    tmp.to_string_lossy()
                );
            }
        }
        self.announced_temps.retain(|n| in_flight.contains(n));
        if batch.is_empty() && in_flight.is_empty() {
            log::trace!(target: LOG, "{}: nothing new", reason.as_str());
        }

        batch.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        for (_, _, event) in batch {
            log::debug!(
                target: LOG,
                "delivering seq={} to {} listener(s)",
                event.seq_id,
                dispatcher.listener_count()
            );
            dispatcher.dispatch(event);
        }
        // Forget files the producer pruned so the sets stay bounded.
        self.seen.retain(|n| present.contains(n));
        self.failures.retain(|n, _| present.contains(n));
    }
}

enum ReadError {
    Gone,
    Rejected(String),
    Transient(io::Error),
}

fn read_event(path: &Path, seq_from_name: u64, max: usize) -> Result<DetectionEvent, ReadError> {
    let io_err = |e: io::Error| {
        if e.kind() == io::ErrorKind::NotFound {
            ReadError::Gone
        } else {
            ReadError::Transient(e)
        }
    };
    let meta = fs::symlink_metadata(path).map_err(io_err)?;
    if !meta.file_type().is_file() {
        return Err(ReadError::Rejected("not a regular file".into()));
    }
    if meta.len() > max as u64 {
        return Err(ReadError::Rejected(format!(
            "{} bytes exceeds limit {max}",
            meta.len()
        )));
    }
    let mut buf = Vec::with_capacity(meta.len() as usize);
    fs::File::open(path)
        .map_err(io_err)?
        .take(max as u64 + 1)
        .read_to_end(&mut buf)
        .map_err(io_err)?;
    if buf.len() > max {
        return Err(ReadError::Rejected(format!("grew beyond limit {max}")));
    }
    let event: DetectionEvent =
        serde_json::from_slice(&buf).map_err(|e| ReadError::Rejected(format!("bad JSON: {e}")))?;
    event
        .validate()
        .map_err(|e| ReadError::Rejected(e.to_string()))?;
    if event.seq_id != seq_from_name {
        return Err(ReadError::Rejected(format!(
            "seq_id {} does not match file name ({seq_from_name})",
            event.seq_id
        )));
    }
    Ok(event)
}

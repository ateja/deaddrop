use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::io::{self, Read};
use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use notify::{RecommendedWatcher, RecursiveMode, Watcher};

use super::{naming, security};
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

        let (tx, rx) = mpsc::channel();
        let wake = tx.clone();
        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                if let Err(e) = &res {
                    log::warn!("deaddrop: watcher reported {e}; rescanning");
                }
                let _ = wake.send(Signal::Wake);
            })?;
        // Register the watch before the initial scan: anything renamed in
        // after this point triggers a wakeup, anything before it is listed.
        watcher.watch(&self.config.dir, RecursiveMode::NonRecursive)?;

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
                            scanner.scan(&dispatcher);
                        }
                        Err(RecvTimeoutError::Timeout) => scanner.scan(&dispatcher),
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
}

impl Scanner {
    fn new(config: ConsumerConfig) -> Self {
        Self {
            config,
            seen: HashSet::new(),
            failures: HashMap::new(),
        }
    }

    fn initial(&mut self, dispatcher: &Dispatcher) {
        if self.config.replay_existing {
            self.scan(dispatcher);
        } else if let Ok(entries) = fs::read_dir(&self.config.dir) {
            self.seen.extend(
                entries
                    .flatten()
                    .map(|e| e.file_name())
                    .filter(|n| n.to_str().and_then(naming::parse_event_file_name).is_some()),
            );
        }
    }

    fn scan(&mut self, dispatcher: &Dispatcher) {
        let entries = match fs::read_dir(&self.config.dir) {
            Ok(e) => e,
            Err(e) => {
                log::warn!("deaddrop: cannot list {}: {e}", self.config.dir.display());
                return;
            }
        };

        let mut present = HashSet::new();
        let mut batch: Vec<(u64, OsString, DetectionEvent)> = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(seq_id) = name.to_str().and_then(naming::parse_event_file_name) else {
                continue;
            };
            present.insert(name.clone());
            if self.seen.contains(&name) {
                continue;
            }
            match read_event(&entry.path(), seq_id, self.config.max_event_bytes) {
                Ok(event) => {
                    self.seen.insert(name.clone());
                    self.failures.remove(&name);
                    batch.push((seq_id, name, event));
                }
                Err(ReadError::Gone) => {
                    present.remove(&name);
                }
                Err(ReadError::Rejected(why)) => {
                    log::warn!("deaddrop: skipping {}: {why}", entry.path().display());
                    self.seen.insert(name);
                }
                Err(ReadError::Transient(e)) => {
                    let n = self.failures.entry(name.clone()).or_insert(0);
                    *n += 1;
                    if *n >= MAX_READ_ATTEMPTS {
                        log::warn!(
                            "deaddrop: giving up on {} after {n} attempts: {e}",
                            entry.path().display()
                        );
                        self.failures.remove(&name);
                        self.seen.insert(name);
                    }
                }
            }
        }

        batch.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        for (_, _, event) in batch {
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

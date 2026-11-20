use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use super::{atomic, naming, security};
use crate::logging::PRODUCER as LOG;
use crate::transport::ProducerTransport;
use crate::{DetectionEvent, Error, ProducerConfig, Result};

/// Temp files older than this at startup are leftovers from a crash.
const STALE_TEMP_AGE: Duration = Duration::from_secs(60);
/// Attempts at finding a free final name (collisions need equal 64 bit nonces).
const RENAME_ATTEMPTS: usize = 4;
/// Retention runs once per this many publishes, so a publish stays O(1).
/// The directory can therefore briefly hold `max_files + PRUNE_EVERY` files.
pub const PRUNE_EVERY: u32 = 64;

/// Writes each event as `evt-<seq>-<nonce>.json` via temp file + rename.
pub struct FileProducer {
    config: ProducerConfig,
    /// Serializes retention so two threads do not prune the same files.
    prune_lock: Mutex<()>,
    since_prune: AtomicU32,
}

impl FileProducer {
    pub fn new(config: ProducerConfig) -> Result<Self> {
        config.check()?;
        let existed = config.dir.exists();
        security::prepare_producer_dir(&config)?;
        log::debug!(
            target: LOG,
            "shared directory {} ({}; durable={}, max_files={:?})",
            config.dir.display(),
            if existed { "already existed, ownership and permissions verified" } else { "created with restrictive permissions" },
            config.durable,
            config.max_files,
        );
        let producer = Self {
            config,
            prune_lock: Mutex::new(()),
            since_prune: AtomicU32::new(0),
        };
        producer.remove_stale_temps();
        producer.prune();
        Ok(producer)
    }

    pub fn dir(&self) -> &std::path::Path {
        &self.config.dir
    }

    fn unix_mode(&self) -> u32 {
        #[cfg(unix)]
        return self.config.file_mode;
        #[cfg(not(unix))]
        0
    }

    fn unix_group(&self) -> Option<u32> {
        #[cfg(unix)]
        return self.config.group;
        #[cfg(not(unix))]
        None
    }

    fn remove_stale_temps(&self) {
        let Ok(entries) = fs::read_dir(&self.config.dir) else {
            return;
        };
        let now = SystemTime::now();
        for entry in entries.flatten() {
            let name = entry.file_name();
            if !name.to_str().is_some_and(naming::is_temp_file_name) {
                continue;
            }
            let stale = entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| now.duration_since(t).ok())
                .is_some_and(|age| age >= STALE_TEMP_AGE);
            if stale && fs::remove_file(entry.path()).is_ok() {
                log::debug!(target: LOG, "removed stale temp file {} left by an earlier crash", entry.path().display());
            }
        }
    }

    /// Applies `max_age` and `max_files`. Failures are logged, not returned:
    /// retention must never block publishing.
    fn prune(&self) {
        if self.config.max_files.is_none() && self.config.max_age.is_none() {
            return;
        }
        let _guard = self.prune_lock.lock().unwrap_or_else(|p| p.into_inner());
        let entries = match fs::read_dir(&self.config.dir) {
            Ok(e) => e,
            Err(e) => {
                log::warn!(target: LOG, "retention scan failed: {e}");
                return;
            }
        };
        let mut files: Vec<(SystemTime, String, PathBuf)> = entries
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().into_string().ok()?;
                naming::parse_event_file_name(&name)?;
                let modified = entry.metadata().and_then(|m| m.modified()).ok()?;
                Some((modified, name, entry.path()))
            })
            .collect();
        files.sort();

        let now = SystemTime::now();
        let expired = self.config.max_age.map_or(0, |max_age| {
            files
                .iter()
                .take_while(|(t, _, _)| now.duration_since(*t).is_ok_and(|age| age > max_age))
                .count()
        });
        let excess = self
            .config
            .max_files
            .map_or(0, |max| files.len().saturating_sub(max));
        let mut removed = 0usize;
        for (_, _, path) in files.iter().take(expired.max(excess)) {
            match fs::remove_file(path) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => log::warn!(target: LOG, "could not prune {}: {e}", path.display()),
            }
        }
        if removed > 0 {
            log::debug!(
                target: LOG,
                "retention: removed {removed} old event file(s), {} kept",
                files.len() - removed
            );
        }
    }
}

impl ProducerTransport for FileProducer {
    fn publish(&self, event: &DetectionEvent) -> Result<()> {
        let bytes = serde_json::to_vec(event)?;
        if bytes.len() > self.config.max_event_bytes {
            return Err(Error::EventTooLarge {
                size: bytes.len(),
                max: self.config.max_event_bytes,
            });
        }

        let dir = &self.config.dir;
        let seq = event.seq_id;
        let started = Instant::now();
        // Temp file lives in the target directory so the rename never
        // crosses a volume boundary (which would not be atomic).
        let tmp = loop {
            let candidate = dir.join(naming::temp_file_name(naming::nonce()));
            log::debug!(
                target: LOG,
                "seq={seq} writing {} bytes to temp file {} (hidden from consumers until renamed)",
                bytes.len(),
                candidate.display()
            );
            match atomic::write_new_file(
                &candidate,
                &bytes,
                self.config.durable,
                self.unix_mode(),
                self.unix_group(),
            ) {
                Ok(()) => {
                    log::debug!(
                        target: LOG,
                        "seq={seq} temp file complete{} in {:.2} ms",
                        if self.config.durable { " and flushed to disk (fsync)" } else { "" },
                        ms(started)
                    );
                    break candidate;
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    let _ = fs::remove_file(&candidate);
                    return Err(e.into());
                }
            }
        };

        let mut last_err = None;
        for _ in 0..RENAME_ATTEMPTS {
            let target = dir.join(naming::event_file_name(seq, naming::nonce()));
            log::debug!(
                target: LOG,
                "seq={seq} atomic rename {} -> {}",
                file_name(&tmp),
                file_name(&target)
            );
            let renaming = Instant::now();
            match atomic::rename_no_replace(&tmp, &target) {
                Ok(()) => {
                    let rename_ms = ms(renaming);
                    if self.config.durable {
                        atomic::sync_dir(dir)?;
                    }
                    log::debug!(
                        target: LOG,
                        "seq={seq} rename done in {rename_ms:.2} ms: event is now visible to consumers as {} (publish took {:.2} ms)",
                        target.display(),
                        ms(started)
                    );
                    if self.since_prune.fetch_add(1, Ordering::Relaxed) + 1 >= PRUNE_EVERY {
                        self.since_prune.store(0, Ordering::Relaxed);
                        self.prune();
                    }
                    return Ok(());
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    log::debug!(target: LOG, "seq={seq} {} already exists; retrying with a new name", file_name(&target));
                    last_err = Some(e);
                }
                Err(e) => {
                    last_err = Some(e);
                    break;
                }
            }
        }
        let _ = fs::remove_file(&tmp);
        Err(last_err
            .unwrap_or_else(|| io::Error::other("rename failed"))
            .into())
    }

    fn shutdown(&self) -> Result<()> {
        // No handles are held between publishes. Apply retention once more
        // so a clean exit leaves the directory within limits.
        self.prune();
        Ok(())
    }
}

fn ms(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1e3
}

fn file_name(path: &std::path::Path) -> std::borrow::Cow<'_, str> {
    path.file_name()
        .map(|n| n.to_string_lossy())
        .unwrap_or_else(|| path.to_string_lossy())
}

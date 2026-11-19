use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use super::{atomic, naming, security};
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
        security::prepare_producer_dir(&config)?;
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
            if stale {
                let _ = fs::remove_file(entry.path());
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
                log::warn!("deaddrop: retention scan failed: {e}");
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
        for (_, _, path) in files.iter().take(expired.max(excess)) {
            if let Err(e) = fs::remove_file(path) {
                if e.kind() != io::ErrorKind::NotFound {
                    log::warn!("deaddrop: could not prune {}: {e}", path.display());
                }
            }
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
        // Temp file lives in the target directory so the rename never
        // crosses a volume boundary (which would not be atomic).
        let tmp = loop {
            let candidate = dir.join(naming::temp_file_name(naming::nonce()));
            match atomic::write_new_file(
                &candidate,
                &bytes,
                self.config.durable,
                self.unix_mode(),
                self.unix_group(),
            ) {
                Ok(()) => break candidate,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    let _ = fs::remove_file(&candidate);
                    return Err(e.into());
                }
            }
        };

        let mut last_err = None;
        for _ in 0..RENAME_ATTEMPTS {
            let target = dir.join(naming::event_file_name(event.seq_id, naming::nonce()));
            match atomic::rename_no_replace(&tmp, &target) {
                Ok(()) => {
                    if self.config.durable {
                        atomic::sync_dir(dir)?;
                    }
                    if self.since_prune.fetch_add(1, Ordering::Relaxed) + 1 >= PRUNE_EVERY {
                        self.since_prune.store(0, Ordering::Relaxed);
                        self.prune();
                    }
                    return Ok(());
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => last_err = Some(e),
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

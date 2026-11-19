use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::file::FileProducer;
use crate::transport::ProducerTransport;
use crate::{DetectionEvent, Error, Result, DEFAULT_MAX_EVENT_BYTES};

/// Settings for the file based producer.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Shared directory. Created with restrictive permissions if missing.
    pub dir: PathBuf,
    /// `fsync` the event file and the directory on every publish. Costs a few
    /// milliseconds on spinning media, well under one on SSDs. Turn off only
    /// if losing the last events on power loss is acceptable.
    pub durable: bool,
    /// Reject events whose JSON exceeds this many bytes.
    pub max_event_bytes: usize,
    /// Keep at most this many event files; oldest are pruned first.
    pub max_files: Option<usize>,
    /// Prune event files older than this.
    pub max_age: Option<Duration>,
    /// Unix mode for a directory the producer creates. Must not grant group
    /// or other write. Use `0o750` together with [`Self::group`] to limit
    /// readers to one group.
    #[cfg(unix)]
    pub dir_mode: u32,
    /// Unix mode for event files. Must not grant group or other write.
    #[cfg(unix)]
    pub file_mode: u32,
    /// Unix group to assign to the directory and every event file.
    #[cfg(unix)]
    pub group: Option<u32>,
    /// SDDL applied to a directory the producer creates. The default grants
    /// SYSTEM and Administrators full control, Authenticated Users read and
    /// traverse, and blocks inheritance from the parent.
    #[cfg(windows)]
    pub dir_sddl: String,
}

/// Default Windows DACL: protected, SYSTEM + Administrators full control,
/// Authenticated Users read/execute, inherited by files created inside.
#[cfg(windows)]
pub const DEFAULT_DIR_SDDL: &str = "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FRFX;;;AU)";

impl ProducerConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            durable: true,
            max_event_bytes: DEFAULT_MAX_EVENT_BYTES,
            max_files: Some(1024),
            max_age: Some(Duration::from_secs(24 * 60 * 60)),
            #[cfg(unix)]
            dir_mode: 0o755,
            #[cfg(unix)]
            file_mode: 0o644,
            #[cfg(unix)]
            group: None,
            #[cfg(windows)]
            dir_sddl: DEFAULT_DIR_SDDL.to_owned(),
        }
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.dir.as_os_str().is_empty() {
            return Err(Error::InvalidConfig("dir is empty".into()));
        }
        if self.max_event_bytes == 0 {
            return Err(Error::InvalidConfig("max_event_bytes must be > 0".into()));
        }
        if self.max_files == Some(0) {
            return Err(Error::InvalidConfig("max_files must be > 0 or None".into()));
        }
        #[cfg(unix)]
        {
            if self.dir_mode & 0o022 != 0 || self.dir_mode & !0o7777 != 0 {
                return Err(Error::InvalidConfig(format!(
                    "dir_mode {:o} must not grant group/other write",
                    self.dir_mode
                )));
            }
            if self.file_mode & 0o022 != 0 || self.file_mode & !0o777 != 0 {
                return Err(Error::InvalidConfig(format!(
                    "file_mode {:o} must not grant group/other write",
                    self.file_mode
                )));
            }
        }
        Ok(())
    }
}

/// Publishes events. Safe to share across threads (`&self` methods).
pub struct EventProducer {
    transport: Box<dyn ProducerTransport>,
    closed: AtomicBool,
}

impl EventProducer {
    /// Opens (or securely creates) `dir` with default settings.
    pub fn init(dir: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(ProducerConfig::new(dir.as_ref()))
    }

    /// Opens the file transport with explicit settings.
    pub fn with_config(config: ProducerConfig) -> Result<Self> {
        Ok(Self::with_transport(Box::new(FileProducer::new(config)?)))
    }

    /// Uses any transport, for alternative backends or tests.
    pub fn with_transport(transport: Box<dyn ProducerTransport>) -> Self {
        Self {
            transport,
            closed: AtomicBool::new(false),
        }
    }

    /// Validates and publishes one event. When this returns `Ok`, the event
    /// file is complete and visible to consumers.
    pub fn publish_event(&self, event: &DetectionEvent) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::ShutDown);
        }
        event.validate()?;
        self.transport.publish(event)
    }

    /// Releases handles. Further publishes fail with [`Error::ShutDown`].
    /// Calling it more than once is harmless.
    pub fn shutdown(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.transport.shutdown()
    }

    pub fn is_shut_down(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl Drop for EventProducer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::file::FileConsumer;
use crate::transport::{ConsumerTransport, Dispatcher};
use crate::{DetectionEvent, Error, Result, DEFAULT_MAX_EVENT_BYTES};

/// Who must own the shared directory before a consumer will trust it.
///
/// This blocks directory squatting: a local user who pre-creates the path
/// cannot feed forged events to the consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustedOwner {
    /// `root` on Unix; `SYSTEM` or `BUILTIN\Administrators` on Windows.
    Privileged,
    /// The account the consumer itself runs as.
    CurrentUser,
    /// A specific Unix uid, for example a dedicated service account.
    #[cfg(unix)]
    Uid(u32),
    /// Skip the owner check. Development only.
    Any,
}

/// Settings for the file based consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub dir: PathBuf,
    pub trusted_owner: TrustedOwner,
    /// Deliver events already present when watching starts. Consumers must
    /// therefore be idempotent on `seq_id` across restarts.
    pub replay_existing: bool,
    /// Safety net rescan in case the OS drops or coalesces notifications
    /// (FSEvents can, under load). `None` relies on the watcher alone.
    pub rescan_interval: Option<Duration>,
    /// Files larger than this are skipped unread.
    pub max_event_bytes: usize,
}

impl ConsumerConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            trusted_owner: TrustedOwner::Privileged,
            replay_existing: true,
            rescan_interval: Some(Duration::from_secs(5)),
            max_event_bytes: DEFAULT_MAX_EVENT_BYTES,
        }
    }
}

/// Receives events and invokes registered listeners on a background thread.
pub struct EventConsumer {
    transport: Box<dyn ConsumerTransport>,
    dispatcher: Dispatcher,
    watching: bool,
    closed: bool,
}

impl EventConsumer {
    /// Targets `dir` with default settings (directory must be owned by a
    /// privileged account).
    pub fn init(dir: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(ConsumerConfig::new(dir.as_ref()))
    }

    pub fn with_config(config: ConsumerConfig) -> Result<Self> {
        if config.dir.as_os_str().is_empty() {
            return Err(Error::InvalidConfig("dir is empty".into()));
        }
        if config.max_event_bytes == 0 {
            return Err(Error::InvalidConfig("max_event_bytes must be > 0".into()));
        }
        Ok(Self::with_transport(Box::new(FileConsumer::new(config))))
    }

    pub fn with_transport(transport: Box<dyn ConsumerTransport>) -> Self {
        Self {
            transport,
            dispatcher: Dispatcher::new(),
            watching: false,
            closed: false,
        }
    }

    /// Adds a listener. Listeners run on the watcher thread, one event at a
    /// time, in `seq_id` order within each batch. Keep them short or hand
    /// work off to your own queue. May be called before or after
    /// [`Self::start_watching`].
    pub fn register_event_listener<F>(&mut self, listener: F) -> Result<()>
    where
        F: Fn(DetectionEvent) + Send + Sync + 'static,
    {
        if self.closed {
            return Err(Error::ShutDown);
        }
        self.dispatcher.add_listener(Arc::new(listener));
        Ok(())
    }

    /// Validates the directory, registers the OS watcher and returns. Events
    /// are delivered asynchronously until [`Self::shutdown`].
    pub fn start_watching(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::ShutDown);
        }
        if self.watching {
            return Err(Error::AlreadyWatching);
        }
        self.transport.start(self.dispatcher.clone())?;
        self.watching = true;
        Ok(())
    }

    pub fn is_watching(&self) -> bool {
        self.watching
    }

    /// Stops the watcher and joins the worker thread. Safe to call more than
    /// once, and safe to call from inside a listener.
    pub fn shutdown(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.watching = false;
        self.transport.stop()
    }
}

impl Drop for EventConsumer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

use std::path::PathBuf;

/// Result alias used across the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Every failure the library can report.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("invalid event: {0}")]
    InvalidEvent(String),

    #[error("event is {size} bytes, limit is {max}")]
    EventTooLarge { size: usize, max: usize },

    #[error("insecure IPC directory {path}: {reason}")]
    InsecureDirectory { path: PathBuf, reason: String },

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("file system watcher error: {0}")]
    Watcher(String),

    #[error("consumer is already watching")]
    AlreadyWatching,

    #[error("endpoint has been shut down")]
    ShutDown,
}

impl Error {
    pub(crate) fn insecure(path: &std::path::Path, reason: impl Into<String>) -> Self {
        Error::InsecureDirectory {
            path: path.to_path_buf(),
            reason: reason.into(),
        }
    }
}

impl From<notify::Error> for Error {
    fn from(e: notify::Error) -> Self {
        Error::Watcher(e.to_string())
    }
}

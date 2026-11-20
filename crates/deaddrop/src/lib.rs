//! # deaddrop
//!
//! Near real time, decoupled event exchange between local endpoint
//! applications on Windows and macOS (Linux works too, mostly for development).
//!
//! The producer and the consumer never reference each other. Both bind to
//! [`EventProducer`] / [`EventConsumer`], which delegate to a pluggable
//! transport ([`ProducerTransport`] / [`ConsumerTransport`]). The shipped
//! transport is file based:
//!
//! * **Producer**: serializes the event into a hidden temp file in the shared
//!   directory (exclusive create), flushes it, then atomically renames it to
//!   its final name without ever replacing an existing file.
//! * **Consumer**: registers a kernel file system watcher (FSEvents on macOS,
//!   `ReadDirectoryChangesW` on Windows, inotify on Linux). Each wakeup
//!   triggers a directory scan, and only complete, renamed event files are
//!   read and dispatched to the registered listeners, in `seq_id` order.
//!
//! ```no_run
//! use deaddrop::{DetectionEvent, EventConsumer, EventProducer};
//!
//! # fn main() -> deaddrop::Result<()> {
//! let producer = EventProducer::init("/var/run/vendor_ipc/events")?;
//! producer.publish_event(&DetectionEvent::new(1, "TargetApp"))?;
//! producer.shutdown()?;
//!
//! let mut consumer = EventConsumer::init("/var/run/vendor_ipc/events")?;
//! consumer.register_event_listener(|event| println!("{}", event.app_name))?;
//! consumer.start_watching()?;
//! // ... later
//! consumer.shutdown()?;
//! # Ok(()) }
//! ```

mod consumer;
mod error;
mod event;
pub mod file;
pub mod logging;
mod producer;
mod transport;

pub use consumer::{ConsumerConfig, EventConsumer, TrustedOwner};
pub use error::{Error, Result};
pub use event::{now_rfc3339_utc, DetectionEvent, SCHEMA_VERSION, SUPPORTED_SCHEMA_MAJOR};
pub use producer::{EventProducer, ProducerConfig};
pub use transport::{ConsumerTransport, Dispatcher, EventCallback, ProducerTransport};

/// Default upper bound on the serialized size of one event (1 MiB).
pub const DEFAULT_MAX_EVENT_BYTES: usize = 1024 * 1024;

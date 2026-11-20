use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, RwLock};

use crate::{DetectionEvent, Result};

/// Listener invoked for every received event, on the transport's thread.
pub type EventCallback = Arc<dyn Fn(DetectionEvent) + Send + Sync + 'static>;

/// Backend a producer writes through. Implement this to swap file IPC for
/// named pipes or Unix sockets without touching [`crate::EventProducer`] callers.
pub trait ProducerTransport: Send + Sync {
    /// Delivers one already validated event.
    fn publish(&self, event: &DetectionEvent) -> Result<()>;
    /// Releases resources. Must be idempotent.
    fn shutdown(&self) -> Result<()>;
}

/// Backend a consumer reads through.
pub trait ConsumerTransport: Send {
    /// Begins delivering events to `dispatcher`. Must not block.
    fn start(&mut self, dispatcher: Dispatcher) -> Result<()>;
    /// Stops delivery and joins any worker. Must be idempotent.
    fn stop(&mut self) -> Result<()>;
}

/// Fans an event out to every registered listener. A panicking listener is
/// logged and isolated so it cannot kill the watcher thread.
#[derive(Clone, Default)]
pub struct Dispatcher {
    listeners: Arc<RwLock<Vec<EventCallback>>>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_listener(&self, listener: EventCallback) {
        self.listeners
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .push(listener);
    }

    pub fn listener_count(&self) -> usize {
        self.listeners
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }

    pub fn dispatch(&self, event: DetectionEvent) {
        // Snapshot so listeners may register further listeners without deadlock.
        let listeners: Vec<EventCallback> = self
            .listeners
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let seq_id = event.seq_id;
        let mut event = Some(event);
        let last = listeners.len().saturating_sub(1);
        for (i, listener) in listeners.iter().enumerate() {
            let ev = if i == last {
                event.take().expect("event consumed once")
            } else {
                event.clone().expect("event present")
            };
            if catch_unwind(AssertUnwindSafe(|| listener(ev))).is_err() {
                log::error!(
                    target: crate::logging::CONSUMER,
                    "listener {i} panicked while handling seq_id {seq_id}"
                );
            }
        }
    }
}

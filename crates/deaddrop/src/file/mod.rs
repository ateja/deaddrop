//! File based transport: atomic write and rename on the producer side,
//! kernel file system watching on the consumer side.
//!
//! On disk layout inside the shared directory:
//!
//! ```text
//! evt-00000000000000010492-9f3c2a7d11e04b58.json   complete event (seq_id, random nonce)
//! .tmp-5be1c9d0a2f34e17.part                       in flight write, ignored by consumers
//! ```
//!
//! The seq_id is zero padded to 20 digits so lexical order equals numeric
//! order. The nonce makes names unique even when a producer reuses a seq_id,
//! and the rename never replaces an existing file.

mod atomic;
mod consumer;
mod naming;
mod producer;
mod security;

pub use consumer::FileConsumer;
pub use naming::{event_file_name, parse_event_file_name};
pub use producer::{FileProducer, PRUNE_EVERY};

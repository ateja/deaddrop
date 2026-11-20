//! Framing for byte-stream channels (named pipes, Unix sockets).
//!
//! A connection starts with the 4 byte magic `DDP1`, then carries frames of
//! a little-endian `u32` length followed by that many bytes of event JSON.

use std::io::{self, Read, Write};

use super::{Incoming, Sink};
use crate::logging::CONSUMER;

pub(super) const MAGIC: &[u8; 4] = b"DDP1";

/// Writes one frame and flushes it.
pub(super) fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "event too large for a frame"))?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(payload);
    w.write_all(&frame)?;
    w.flush()
}

/// Reads frames until the peer disconnects, forwarding each to `sink`.
pub(super) fn read_frames(mut r: impl Read, peer: &str, max_bytes: usize, sink: &Sink) {
    let mut magic = [0u8; 4];
    if let Err(e) = r.read_exact(&mut magic) {
        log::debug!(target: CONSUMER, "{peer} disconnected before the handshake ({e})");
        return;
    }
    if &magic != MAGIC {
        log::warn!(target: CONSUMER, "{peer} is not a deaddrop producer (bad handshake); closing");
        return;
    }
    let mut frames = 0u64;
    loop {
        let mut len = [0u8; 4];
        match r.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                log::debug!(target: CONSUMER, "{peer}: connection ended ({e})");
                break;
            }
        }
        let len = u32::from_le_bytes(len) as usize;
        if len > max_bytes {
            log::warn!(target: CONSUMER, "{peer} sent a {len} byte frame (limit {max_bytes}); closing");
            break;
        }
        let mut bytes = vec![0u8; len];
        if let Err(e) = r.read_exact(&mut bytes) {
            log::debug!(target: CONSUMER, "{peer}: connection ended mid-frame ({e})");
            break;
        }
        frames += 1;
        log::debug!(target: CONSUMER, "read {len} byte frame from {peer}");
        if sink
            .send(Incoming::Payload {
                peer: peer.to_owned(),
                bytes,
            })
            .is_err()
        {
            break; // consumer is stopping
        }
    }
    log::debug!(target: CONSUMER, "{peer} disconnected after {frames} event(s)");
}

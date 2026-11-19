use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const EVENT_PREFIX: &str = "evt-";
const EVENT_SUFFIX: &str = ".json";
pub(crate) const TEMP_PREFIX: &str = ".tmp-";
const TEMP_SUFFIX: &str = ".part";

/// Final name of an event file.
pub fn event_file_name(seq_id: u64, nonce: u64) -> String {
    format!("{EVENT_PREFIX}{seq_id:020}-{nonce:016x}{EVENT_SUFFIX}")
}

/// Returns the seq_id if `name` is a well formed event file name.
pub fn parse_event_file_name(name: &str) -> Option<u64> {
    let rest = name
        .strip_prefix(EVENT_PREFIX)?
        .strip_suffix(EVENT_SUFFIX)?;
    let (seq, nonce) = rest.split_once('-')?;
    let well_formed = seq.len() == 20
        && nonce.len() == 16
        && seq.bytes().all(|b| b.is_ascii_digit())
        && nonce
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    if !well_formed {
        return None;
    }
    seq.parse().ok()
}

pub(crate) fn temp_file_name(nonce: u64) -> String {
    format!("{TEMP_PREFIX}{nonce:016x}{TEMP_SUFFIX}")
}

pub(crate) fn is_temp_file_name(name: &str) -> bool {
    name.starts_with(TEMP_PREFIX) && name.ends_with(TEMP_SUFFIX)
}

/// Unpredictable 64 bit value. `RandomState` is seeded from the OS RNG once
/// per process; the counter and clock make successive values distinct.
pub(crate) fn nonce() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut h = RandomState::new().build_hasher();
    h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    h.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default(),
    );
    h.write_u32(std::process::id());
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let n = event_file_name(10492, 0xdead_beef);
        assert_eq!(n, "evt-00000000000000010492-00000000deadbeef.json");
        assert_eq!(parse_event_file_name(&n), Some(10492));
        assert_eq!(
            parse_event_file_name(&event_file_name(u64::MAX, u64::MAX)),
            Some(u64::MAX)
        );
    }

    #[test]
    fn rejects_lookalikes() {
        for bad in [
            "evt-10492-00000000deadbeef.json",
            "evt-00000000000000010492-00000000DEADBEEF.json",
            "evt-00000000000000010492-00000000deadbeef.json.tmp",
            "evt-99999999999999999999-00000000deadbeef.json", // overflows u64
            ".tmp-00000000deadbeef.part",
            "notes.txt",
        ] {
            assert_eq!(parse_event_file_name(bad), None, "{bad}");
        }
        assert!(is_temp_file_name(&temp_file_name(1)));
    }

    #[test]
    fn nonces_differ() {
        let a: std::collections::HashSet<u64> = (0..10_000).map(|_| nonce()).collect();
        assert_eq!(a.len(), 10_000);
    }
}

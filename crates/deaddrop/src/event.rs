use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{Error, Result};

/// Schema version written by this library.
pub const SCHEMA_VERSION: &str = "1.0";

/// Consumers accept any `1.x` payload. Minor bumps may add fields; unknown
/// fields are ignored on read so older consumers keep working.
pub const SUPPORTED_SCHEMA_MAJOR: u32 = 1;

const MAX_APP_NAME_CHARS: usize = 1024;

/// One event, as exchanged between endpoint apps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetectionEvent {
    /// `MAJOR.MINOR`. Only the major is checked.
    pub schema_version: String,
    /// Producer assigned, monotonically increasing sequence number.
    pub seq_id: u64,
    /// RFC 3339 UTC timestamp, for example `2026-11-18T17:45:00Z`.
    pub timestamp_utc: String,
    /// Name of the detected application.
    pub app_name: String,
    /// Free form, JSON object valued metadata.
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

impl DetectionEvent {
    /// Builds an event stamped with the current schema version and time.
    pub fn new(seq_id: u64, app_name: impl Into<String>) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_owned(),
            seq_id,
            timestamp_utc: now_rfc3339_utc(),
            app_name: app_name.into(),
            metadata: Map::new(),
        }
    }

    /// Adds one metadata entry (builder style).
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Checks the invariants both sides rely on. Producers call this before
    /// writing and consumers call it after reading.
    pub fn validate(&self) -> Result<()> {
        let major = self
            .schema_version
            .split('.')
            .next()
            .and_then(|m| m.parse::<u32>().ok())
            .ok_or_else(|| {
                Error::InvalidEvent(format!(
                    "malformed schema_version {:?}",
                    self.schema_version
                ))
            })?;
        if major != SUPPORTED_SCHEMA_MAJOR {
            return Err(Error::InvalidEvent(format!(
                "unsupported schema major {major}, expected {SUPPORTED_SCHEMA_MAJOR}"
            )));
        }
        if self.app_name.trim().is_empty() {
            return Err(Error::InvalidEvent("app_name is empty".into()));
        }
        if self.app_name.chars().count() > MAX_APP_NAME_CHARS {
            return Err(Error::InvalidEvent(format!(
                "app_name longer than {MAX_APP_NAME_CHARS} characters"
            )));
        }
        if !is_rfc3339_utc(&self.timestamp_utc) {
            return Err(Error::InvalidEvent(format!(
                "timestamp_utc {:?} is not RFC 3339 UTC (YYYY-MM-DDTHH:MM:SS[.fff]Z)",
                self.timestamp_utc
            )));
        }
        Ok(())
    }

    /// Serializes to compact JSON.
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Parses and validates JSON.
    pub fn from_json(json: &str) -> Result<Self> {
        let event: Self = serde_json::from_str(json)?;
        event.validate()?;
        Ok(event)
    }
}

/// Current wall clock time as `YYYY-MM-DDTHH:MM:SS.mmmZ`.
pub fn now_rfc3339_utc() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format_unix_utc(d.as_secs() as i64, d.subsec_millis())
}

fn format_unix_utc(secs: i64, millis: u32) -> String {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

/// Howard Hinnant's days to civil date algorithm (proleptic Gregorian).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// Structural check for `YYYY-MM-DDTHH:MM:SS[.fraction]Z`.
fn is_rfc3339_utc(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 20 || !matches!(b.last(), Some(b'Z' | b'z')) {
        return false;
    }
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    let shape = digits(0..4)
        && b[4] == b'-'
        && digits(5..7)
        && b[7] == b'-'
        && digits(8..10)
        && matches!(b[10], b'T' | b't')
        && digits(11..13)
        && b[13] == b':'
        && digits(14..16)
        && b[16] == b':'
        && digits(17..19);
    if !shape {
        return false;
    }
    let frac = &b[19..b.len() - 1];
    let frac_ok = frac.is_empty()
        || (frac[0] == b'.' && frac.len() > 1 && frac[1..].iter().all(u8::is_ascii_digit));
    let num = |r: std::ops::Range<usize>| s[r].parse::<u32>().unwrap_or(u32::MAX);
    frac_ok
        && (1..=12).contains(&num(5..7))
        && (1..=31).contains(&num(8..10))
        && num(11..13) <= 23
        && num(14..16) <= 59
        && num(17..19) <= 60
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_known_instants() {
        assert_eq!(format_unix_utc(0, 0), "1970-01-01T00:00:00.000Z");
        assert_eq!(format_unix_utc(951_782_400, 5), "2000-02-29T00:00:00.005Z");
        assert_eq!(
            format_unix_utc(1_795_023_900, 0),
            "2026-11-18T17:45:00.000Z"
        );
        assert!(is_rfc3339_utc(&now_rfc3339_utc()));
    }

    #[test]
    fn timestamp_shapes() {
        assert!(is_rfc3339_utc("2026-11-18T17:45:00Z"));
        assert!(is_rfc3339_utc("2026-11-18T17:45:00.123456Z"));
        assert!(!is_rfc3339_utc("2026-11-18T17:45:00+01:00"));
        assert!(!is_rfc3339_utc("2026-13-18T17:45:00Z"));
        assert!(!is_rfc3339_utc("2026-11-18T17:45:00.Z"));
        assert!(!is_rfc3339_utc("yesterday"));
    }

    #[test]
    fn validation() {
        let ok = DetectionEvent::new(1, "TargetApp").with_metadata("confidence", 98);
        ok.validate().unwrap();

        let mut e = ok.clone();
        e.schema_version = "2.0".into();
        assert!(matches!(e.validate(), Err(Error::InvalidEvent(_))));

        let mut e = ok.clone();
        e.schema_version = "1.7".into();
        e.validate().unwrap();

        let mut e = ok.clone();
        e.app_name = "  ".into();
        assert!(e.validate().is_err());
    }

    #[test]
    fn json_roundtrip_ignores_unknown_fields() {
        let json = r#"{"schema_version":"1.3","seq_id":7,"timestamp_utc":"2026-11-18T17:45:00Z",
                       "app_name":"A","metadata":{"k":[1,2]},"added_in_1_3":true}"#;
        let e = DetectionEvent::from_json(json).unwrap();
        assert_eq!(e.seq_id, 7);
        assert_eq!(e.metadata["k"], serde_json::json!([1, 2]));
        let back = DetectionEvent::from_json(&e.to_json().unwrap()).unwrap();
        assert_eq!(back, e);
    }
}

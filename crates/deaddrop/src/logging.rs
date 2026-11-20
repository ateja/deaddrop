//! Opt-in diagnostic logging to stderr.
//!
//! The library logs through the [`log`] facade with two targets,
//! `deaddrop::producer` and `deaddrop::consumer`. A Rust application can route
//! those through any logger it already uses (`env_logger`, `tracing-log`, ...).
//! Programs without one, which includes everything using the C or Python
//! bindings, get this small stderr logger. The bindings install it
//! automatically; Rust code calls [`init_from_env`].
//!
//! The level comes from the `DEADDROP_LOG` environment variable: `off`,
//! `error`, `warn` (the default), `info`, `debug` or `trace`. At `debug` every
//! step of an exchange is printed: which directory and file the producer
//! writes, the atomic rename, the OS notification the consumer receives,
//! which file it reads and the event it hands to listeners.
//!
//! Each line starts with `[deaddrop:<side>]`, so it is easy to filter:
//!
//! ```text
//! [deaddrop:producer] 06:21:10.004 DEBUG pid=4242 seq=7 atomic rename .tmp-5be1c9d0a2f34e17.part -> evt-00000000000000000007-9f3c2a7d11e04b58.json
//! ```

use std::io::Write;
use std::sync::OnceLock;

use log::{LevelFilter, Log, Metadata, Record};

/// Environment variable read by [`init_from_env`].
pub const ENV_VAR: &str = "DEADDROP_LOG";

/// Marker every log line starts with, for grepping.
pub const LINE_PREFIX: &str = "[deaddrop:";

pub(crate) const PRODUCER: &str = "deaddrop::producer";
pub(crate) const CONSUMER: &str = "deaddrop::consumer";

struct StderrLogger;

static LOGGER: StderrLogger = StderrLogger;
static INSTALLED: OnceLock<bool> = OnceLock::new();

impl Log for StderrLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.target().starts_with("deaddrop") && metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let side = record
            .target()
            .strip_prefix("deaddrop::")
            .unwrap_or(record.target());
        let stamp = crate::now_rfc3339_utc();
        let time = stamp.get(11..23).unwrap_or(&stamp); // HH:MM:SS.mmm
                                                        // One write per line so lines from different threads never interleave.
        let line = format!(
            "{LINE_PREFIX}{side}] {time} {:<5} pid={} {}\n",
            record.level(),
            std::process::id(),
            record.args()
        );
        let _ = std::io::stderr().lock().write_all(line.as_bytes());
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

/// Parses `off`, `error`, `warn`, `info`, `debug` or `trace` (any case).
pub fn parse_level(s: &str) -> Option<LevelFilter> {
    match s.trim().to_ascii_lowercase().as_str() {
        "off" | "none" | "0" => Some(LevelFilter::Off),
        "error" => Some(LevelFilter::Error),
        "warn" | "warning" => Some(LevelFilter::Warn),
        "info" => Some(LevelFilter::Info),
        "debug" => Some(LevelFilter::Debug),
        "trace" => Some(LevelFilter::Trace),
        _ => None,
    }
}

/// Installs the stderr logger with the level from `DEADDROP_LOG` (default
/// `warn`). Returns `false` if the process already has another logger, in
/// which case that logger and its configuration stay in charge.
pub fn init_from_env() -> bool {
    let level = std::env::var(ENV_VAR)
        .ok()
        .and_then(|v| parse_level(&v))
        .unwrap_or(LevelFilter::Warn);
    init_with_level(level)
}

/// Installs the stderr logger (once per process) and sets its level.
/// Returns `false` if another logger is installed.
pub fn init_with_level(level: LevelFilter) -> bool {
    let ours = *INSTALLED.get_or_init(|| log::set_logger(&LOGGER).is_ok());
    if ours {
        log::set_max_level(level);
    }
    ours
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels() {
        assert_eq!(parse_level("DEBUG"), Some(LevelFilter::Debug));
        assert_eq!(parse_level(" warn "), Some(LevelFilter::Warn));
        assert_eq!(parse_level("off"), Some(LevelFilter::Off));
        assert_eq!(parse_level("loud"), None);
    }

    #[test]
    fn install_is_idempotent() {
        assert!(init_with_level(LevelFilter::Debug));
        assert!(init_with_level(LevelFilter::Warn));
        assert_eq!(log::max_level(), LevelFilter::Warn);
    }
}

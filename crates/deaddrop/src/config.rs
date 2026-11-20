//! Runtime transport selection from a YAML file.
//!
//! [`crate::EventProducer::init`] and [`crate::EventConsumer::init`] take one
//! path. They pick the transport like this:
//!
//! 1. If the path is a `.yaml` / `.yml` file, that file is the configuration.
//! 2. Otherwise, if the `DEADDROP_CONFIG` environment variable names a YAML
//!    file, that file is the configuration and the path is the default
//!    shared directory for the file transport.
//! 3. Otherwise the path is the shared directory and the file transport is
//!    used, exactly as before configuration files existed.
//!
//! ```yaml
//! transport: ipc            # file | ipc
//! file:
//!   dir: /var/run/vendor_ipc/events   # optional with DEADDROP_CONFIG
//! ipc:
//!   name: com.example.deaddrop        # pipe / XPC service / socket name
//!   trusted_peer: privileged          # privileged | current_user | any
//!   max_pending: 1024
//!   flush_timeout_ms: 5000
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::{Error, Result, TrustedOwner};

/// Environment variable naming the configuration file.
pub const ENV_VAR: &str = "DEADDROP_CONFIG";

/// Which implementation carries events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportKind {
    /// Files in a shared directory, watched by the OS file watcher.
    #[default]
    File,
    /// Direct OS IPC: named pipes (Windows), XPC (macOS), Unix sockets (other).
    Ipc,
}

/// Peer identity policy as spelled in YAML.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustedPeer {
    Privileged,
    CurrentUser,
    Any,
}

impl From<TrustedPeer> for TrustedOwner {
    fn from(p: TrustedPeer) -> Self {
        match p {
            TrustedPeer::Privileged => TrustedOwner::Privileged,
            TrustedPeer::CurrentUser => TrustedOwner::CurrentUser,
            TrustedPeer::Any => TrustedOwner::Any,
        }
    }
}

/// `file:` section.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSection {
    /// Shared directory. Relative paths are resolved against the YAML file.
    pub dir: Option<PathBuf>,
}

/// `ipc:` section.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IpcSection {
    /// Endpoint name both sides agree on. Letters, digits, `.`, `_`, `-`.
    pub name: Option<String>,
    /// Who may be on the other end of the connection. Consumers default to
    /// the policy their code passes in; producers default to `privileged`.
    pub trusted_peer: Option<TrustedPeer>,
    /// Events a producer holds in memory while the consumer is unreachable.
    pub max_pending: Option<usize>,
    /// How long a producer's `shutdown()` waits to deliver queued events.
    pub flush_timeout_ms: Option<u64>,
}

/// A parsed configuration file.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub transport: TransportKind,
    #[serde(default)]
    pub file: Option<FileSection>,
    #[serde(default)]
    pub ipc: Option<IpcSection>,
}

impl Config {
    /// Reads and validates a YAML file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| {
            Error::InvalidConfig(format!("cannot read config {}: {e}", path.display()))
        })?;
        Self::parse(&text).map_err(|e| Error::InvalidConfig(format!("{}: {e}", path.display())))
    }

    /// Parses YAML text.
    pub fn parse(text: &str) -> std::result::Result<Self, String> {
        let config: Config = serde_norway::from_str(text).map_err(|e| e.to_string())?;
        if let Some(name) = config.ipc.as_ref().and_then(|i| i.name.as_deref()) {
            validate_name(name)?;
        }
        Ok(config)
    }
}

fn validate_name(name: &str) -> std::result::Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "ipc.name {name:?} must be 1-128 characters of letters, digits, '.', '_' or '-'"
        ))
    }
}

/// Default endpoint name when the YAML gives none.
pub const DEFAULT_IPC_NAME: &str = "com.deaddrop.events";

/// Settings the IPC transport needs, after defaults are applied.
#[derive(Debug, Clone)]
pub struct IpcSettings {
    pub name: String,
    pub trusted_peer: Option<TrustedOwner>,
    pub max_pending: usize,
    pub flush_timeout: Duration,
}

impl IpcSettings {
    fn from_section(section: Option<&IpcSection>) -> Self {
        let s = section.cloned().unwrap_or_default();
        Self {
            name: s.name.unwrap_or_else(|| DEFAULT_IPC_NAME.to_owned()),
            trusted_peer: s.trusted_peer.map(Into::into),
            max_pending: s.max_pending.unwrap_or(1024).max(1),
            flush_timeout: Duration::from_millis(s.flush_timeout_ms.unwrap_or(5000)),
        }
    }
}

/// What `init()` should build.
#[derive(Debug, Clone)]
pub(crate) enum Resolved {
    File(PathBuf),
    Ipc(IpcSettings),
}

fn is_yaml_file(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("yaml") || e.eq_ignore_ascii_case("yml"))
}

/// Applies the rules in the module docs to the path given to `init()`.
pub(crate) fn resolve(target: &Path, log_target: &str) -> Result<Resolved> {
    let (config, source, fallback_dir) = if is_yaml_file(target) {
        (Config::load(target)?, target.to_path_buf(), None)
    } else if let Some(env) = std::env::var_os(ENV_VAR).filter(|v| !v.is_empty()) {
        let path = PathBuf::from(env);
        (Config::load(&path)?, path, Some(target.to_path_buf()))
    } else {
        log::debug!(
            target: log_target,
            "no config file ({ENV_VAR} not set): file transport in {}",
            target.display()
        );
        return Ok(Resolved::File(target.to_path_buf()));
    };

    let resolved = match config.transport {
        TransportKind::File => {
            let dir = match config.file.as_ref().and_then(|f| f.dir.clone()) {
                Some(dir) if dir.is_relative() => {
                    source.parent().map_or(dir.clone(), |base| base.join(&dir))
                }
                Some(dir) => dir,
                None => fallback_dir
                    .filter(|d| !d.as_os_str().is_empty())
                    .ok_or_else(|| {
                        Error::InvalidConfig(format!(
                            "{}: transport is file but file.dir is not set",
                            source.display()
                        ))
                    })?,
            };
            Resolved::File(dir)
        }
        TransportKind::Ipc => Resolved::Ipc(IpcSettings::from_section(config.ipc.as_ref())),
    };
    log::debug!(
        target: log_target,
        "config {}: transport={}",
        source.display(),
        match &resolved {
            Resolved::File(dir) => format!("file (directory {})", dir.display()),
            Resolved::Ipc(s) => format!("ipc ({})", crate::ipc::describe_endpoint(&s.name)),
        }
    );
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_and_minimal() {
        let c = Config::parse(
            "transport: ipc\nfile:\n  dir: /x\nipc:\n  name: com.a.b\n  trusted_peer: current_user\n  max_pending: 5\n  flush_timeout_ms: 10\n",
        )
        .unwrap();
        assert_eq!(c.transport, TransportKind::Ipc);
        let s = IpcSettings::from_section(c.ipc.as_ref());
        assert_eq!(s.name, "com.a.b");
        assert_eq!(s.trusted_peer, Some(TrustedOwner::CurrentUser));
        assert_eq!(
            (s.max_pending, s.flush_timeout),
            (5, Duration::from_millis(10))
        );

        // Empty sections (only comments under them) are allowed.
        let c = Config::parse("transport: file\nfile:\n  # dir: /x\nipc:\n").unwrap();
        assert_eq!(c.transport, TransportKind::File);
        assert!(c.file.is_none());
        assert_eq!(Config::parse("").unwrap().transport, TransportKind::File);
    }

    #[test]
    fn rejects_typos_and_bad_names() {
        assert!(Config::parse("transport: pipes\n").is_err());
        assert!(Config::parse("transprot: ipc\n").is_err());
        assert!(Config::parse("ipc:\n  nmae: x\n").is_err());
        assert!(Config::parse("ipc:\n  name: \"a/b\"\n").is_err());
        assert!(Config::parse("ipc:\n  trusted_peer: root\n").is_err());
    }

    #[test]
    fn resolves_relative_dir_against_yaml() {
        let tmp = tempfile::tempdir().unwrap();
        let yaml = tmp.path().join("deaddrop.yaml");
        std::fs::write(&yaml, "transport: file\nfile:\n  dir: events\n").unwrap();
        match resolve(&yaml, "deaddrop::test").unwrap() {
            Resolved::File(dir) => assert_eq!(dir, tmp.path().join("events")),
            other => panic!("{other:?}"),
        }
        std::fs::write(&yaml, "transport: file\n").unwrap();
        assert!(
            resolve(&yaml, "deaddrop::test").is_err(),
            "file.dir required"
        );
    }
}

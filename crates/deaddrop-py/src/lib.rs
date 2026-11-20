//! Python bindings. Build with `maturin build --release` in this directory.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use deaddrop as core;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

create_exception!(
    deaddrop,
    IpcError,
    PyException,
    "Base class for deaddrop errors."
);
create_exception!(
    deaddrop,
    InsecureDirectoryError,
    IpcError,
    "The shared directory failed a trust check."
);
create_exception!(
    deaddrop,
    InvalidEventError,
    IpcError,
    "The event failed validation or is too large."
);
create_exception!(
    deaddrop,
    ShutDownError,
    IpcError,
    "The endpoint was already shut down."
);

fn err(e: core::Error) -> PyErr {
    let msg = e.to_string();
    match e {
        core::Error::InsecureDirectory { .. } => InsecureDirectoryError::new_err(msg),
        core::Error::InvalidEvent(_) | core::Error::EventTooLarge { .. } => {
            InvalidEventError::new_err(msg)
        }
        core::Error::ShutDown => ShutDownError::new_err(msg),
        core::Error::InvalidConfig(_) => PyValueError::new_err(msg),
        _ => IpcError::new_err(msg),
    }
}

fn to_json_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("allow_nan", false)?;
    let text: String = py
        .import("json")?
        .call_method("dumps", (obj,), Some(&kwargs))?
        .extract()?;
    serde_json::from_str(&text).map_err(|e| PyValueError::new_err(e.to_string()))
}

fn from_json_value<'py>(py: Python<'py>, v: &impl serde::Serialize) -> PyResult<Bound<'py, PyAny>> {
    let text = serde_json::to_string(v).map_err(|e| PyValueError::new_err(e.to_string()))?;
    py.import("json")?.call_method1("loads", (text,))
}

/// One event exchanged between endpoint apps.
#[pyclass(name = "DetectionEvent", module = "deaddrop_ipc", frozen, eq)]
#[derive(Clone, PartialEq)]
struct PyDetectionEvent {
    inner: core::DetectionEvent,
}

#[pymethods]
impl PyDetectionEvent {
    #[new]
    #[pyo3(signature = (seq_id, app_name, schema_version = None, timestamp_utc = None, metadata = None))]
    fn new(
        py: Python<'_>,
        seq_id: u64,
        app_name: String,
        schema_version: Option<String>,
        timestamp_utc: Option<String>,
        metadata: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let mut inner = core::DetectionEvent::new(seq_id, app_name);
        if let Some(v) = schema_version {
            inner.schema_version = v;
        }
        if let Some(ts) = timestamp_utc {
            inner.timestamp_utc = ts;
        }
        if let Some(obj) = metadata.filter(|m| !m.is_none()) {
            match to_json_value(py, obj)? {
                serde_json::Value::Object(map) => inner.metadata = map,
                _ => return Err(PyTypeError::new_err("metadata must be a dict")),
            }
        }
        inner.validate().map_err(err)?;
        Ok(Self { inner })
    }

    #[getter]
    fn schema_version(&self) -> &str {
        &self.inner.schema_version
    }
    #[getter]
    fn seq_id(&self) -> u64 {
        self.inner.seq_id
    }
    #[getter]
    fn timestamp_utc(&self) -> &str {
        &self.inner.timestamp_utc
    }
    #[getter]
    fn app_name(&self) -> &str {
        &self.inner.app_name
    }
    /// A fresh dict on every access.
    #[getter]
    fn metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        from_json_value(py, &self.inner.metadata)
    }

    fn to_json(&self) -> PyResult<String> {
        self.inner.to_json().map_err(err)
    }

    #[staticmethod]
    fn from_json(json: &str) -> PyResult<Self> {
        core::DetectionEvent::from_json(json)
            .map(|inner| Self { inner })
            .map_err(err)
    }

    fn __repr__(&self) -> String {
        format!(
            "DetectionEvent(seq_id={}, app_name={:?}, timestamp_utc={:?}, schema_version={:?})",
            self.inner.seq_id,
            self.inner.app_name,
            self.inner.timestamp_utc,
            self.inner.schema_version
        )
    }
}

/// Publishes events into the shared directory.
#[pyclass(name = "EventProducer", module = "deaddrop_ipc", frozen)]
struct PyEventProducer {
    inner: core::EventProducer,
}

#[pymethods]
impl PyEventProducer {
    /// Opens (or securely creates) the shared directory.
    #[staticmethod]
    #[pyo3(signature = (
        path, *, durable = true, max_files = Some(1024), max_age_seconds = Some(86400.0),
        max_event_bytes = core::DEFAULT_MAX_EVENT_BYTES, dir_mode = None, file_mode = None, group = None
    ))]
    #[allow(clippy::too_many_arguments, unused_variables, unused_mut)]
    fn init(
        py: Python<'_>,
        path: PathBuf,
        durable: bool,
        max_files: Option<usize>,
        max_age_seconds: Option<f64>,
        max_event_bytes: usize,
        dir_mode: Option<u32>,
        file_mode: Option<u32>,
        group: Option<u32>,
    ) -> PyResult<Self> {
        // File transport settings; used only if the config selects "file".
        let mut cfg = core::ProducerConfig::new(PathBuf::new());
        cfg.durable = durable;
        cfg.max_files = max_files;
        cfg.max_age = match max_age_seconds {
            Some(s) if !(s.is_finite() && s > 0.0) => {
                return Err(PyValueError::new_err("max_age_seconds must be positive"))
            }
            other => other.map(Duration::from_secs_f64),
        };
        cfg.max_event_bytes = max_event_bytes;
        #[cfg(unix)]
        {
            if let Some(m) = dir_mode {
                cfg.dir_mode = m;
            }
            if let Some(m) = file_mode {
                cfg.file_mode = m;
            }
            cfg.group = group;
        }
        let inner = py
            .allow_threads(|| {
                core::EventProducer::init_with(&path, move |c| {
                    let dir = std::mem::take(&mut c.dir);
                    *c = cfg;
                    c.dir = dir;
                })
            })
            .map_err(err)?;
        Ok(Self { inner })
    }

    /// Atomically writes one event. Releases the GIL during I/O.
    fn publish_event(&self, py: Python<'_>, event: PyRef<'_, PyDetectionEvent>) -> PyResult<()> {
        let ev = event.inner.clone();
        drop(event);
        py.allow_threads(|| self.inner.publish_event(&ev))
            .map_err(err)
    }

    fn shutdown(&self) -> PyResult<()> {
        self.inner.shutdown().map_err(err)
    }

    #[getter]
    fn closed(&self) -> bool {
        self.inner.is_shut_down()
    }

    /// Transport in use, e.g. "file: /path" or "ipc: named pipe \\.\pipe\name".
    #[getter]
    fn endpoint(&self) -> String {
        self.inner.endpoint()
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_exc))]
    fn __exit__(&self, _exc: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<bool> {
        self.shutdown()?;
        Ok(false)
    }
}

/// Watches the shared directory and calls listeners on a background thread.
#[pyclass(name = "EventConsumer", module = "deaddrop_ipc", frozen)]
struct PyEventConsumer {
    // None once shut down. The lock is never held while joining the worker.
    inner: Mutex<Option<core::EventConsumer>>,
    endpoint: String,
}

impl PyEventConsumer {
    fn with<R>(&self, f: impl FnOnce(&mut core::EventConsumer) -> core::Result<R>) -> PyResult<R> {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let c = guard
            .as_mut()
            .ok_or_else(|| ShutDownError::new_err("consumer was shut down"))?;
        f(c).map_err(err)
    }

    fn take(&self) -> Option<core::EventConsumer> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

#[pymethods]
impl PyEventConsumer {
    /// `trusted_owner`: "privileged" (default), "current_user", "any", or a
    /// Unix uid.
    #[staticmethod]
    #[pyo3(signature = (
        path, *, trusted_owner = None, replay_existing = true,
        rescan_interval_seconds = Some(5.0), max_event_bytes = core::DEFAULT_MAX_EVENT_BYTES
    ))]
    fn init(
        path: PathBuf,
        trusted_owner: Option<&Bound<'_, PyAny>>,
        replay_existing: bool,
        rescan_interval_seconds: Option<f64>,
        max_event_bytes: usize,
    ) -> PyResult<Self> {
        let mut cfg = core::ConsumerConfig::new(PathBuf::new());
        cfg.trusted_owner = match trusted_owner {
            None => core::TrustedOwner::Privileged,
            Some(v) => {
                if let Ok(s) = v.extract::<String>() {
                    match s.as_str() {
                        "privileged" => core::TrustedOwner::Privileged,
                        "current_user" => core::TrustedOwner::CurrentUser,
                        "any" => core::TrustedOwner::Any,
                        _ => {
                            return Err(PyValueError::new_err(format!(
                                "unknown trusted_owner {s:?}"
                            )))
                        }
                    }
                } else {
                    #[cfg(unix)]
                    {
                        core::TrustedOwner::Uid(v.extract::<u32>()?)
                    }
                    #[cfg(not(unix))]
                    {
                        return Err(PyTypeError::new_err(
                            "trusted_owner must be a string on Windows",
                        ));
                    }
                }
            }
        };
        cfg.replay_existing = replay_existing;
        cfg.rescan_interval = match rescan_interval_seconds {
            Some(s) if !(s.is_finite() && s > 0.0) => {
                return Err(PyValueError::new_err(
                    "rescan_interval_seconds must be positive",
                ))
            }
            other => other.map(Duration::from_secs_f64),
        };
        cfg.max_event_bytes = max_event_bytes;
        let inner = core::EventConsumer::init_with(&path, move |c| {
            let dir = std::mem::take(&mut c.dir);
            *c = cfg;
            c.dir = dir;
        })
        .map_err(err)?;
        let endpoint = inner.endpoint();
        Ok(Self {
            inner: Mutex::new(Some(inner)),
            endpoint,
        })
    }

    /// `callback(event: DetectionEvent)` runs on the watcher thread with the
    /// GIL held. Exceptions are printed and do not stop delivery.
    fn register_event_listener(&self, py: Python<'_>, callback: PyObject) -> PyResult<()> {
        if !callback.bind(py).is_callable() {
            return Err(PyTypeError::new_err("callback must be callable"));
        }
        self.with(move |c| {
            c.register_event_listener(move |event| {
                Python::with_gil(|py| {
                    if let Err(e) = callback.call1(py, (PyDetectionEvent { inner: event },)) {
                        e.print(py);
                    }
                });
            })
        })
    }

    /// Transport in use, e.g. "file: /path" or "ipc: XPC service com.example".
    #[getter]
    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    fn start_watching(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.with(|c| c.start_watching()))
    }

    /// Stops watching and joins the worker. The GIL is released so a
    /// callback that is waiting for it can finish.
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        match self.take() {
            Some(mut c) => py.allow_threads(move || c.shutdown()).map_err(err),
            None => Ok(()),
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_exc))]
    fn __exit__(&self, py: Python<'_>, _exc: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<bool> {
        self.shutdown(py)?;
        Ok(false)
    }
}

impl Drop for PyEventConsumer {
    fn drop(&mut self) {
        if let Some(mut c) = self.take() {
            // Dropped by the GC with the GIL held: release it while joining,
            // otherwise an in-flight callback waiting for the GIL deadlocks.
            Python::with_gil(|py| py.allow_threads(move || drop(c.shutdown())));
        }
    }
}

/// Sets the diagnostic log level: "off", "error", "warn" (default), "info",
/// "debug" or "trace". Overrides the DEADDROP_LOG environment variable.
/// Lines go to stderr and start with "[deaddrop:producer]" or
/// "[deaddrop:consumer]".
#[pyfunction]
fn set_log_level(level: &str) -> PyResult<()> {
    let parsed = core::logging::parse_level(level).ok_or_else(|| {
        PyValueError::new_err(format!(
            "unknown log level {level:?}; use off, error, warn, info, debug or trace"
        ))
    })?;
    core::logging::init_with_level(parsed);
    Ok(())
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    // Library diagnostics go to stderr at the level in DEADDROP_LOG.
    core::logging::init_from_env();
    m.add_function(wrap_pyfunction!(set_log_level, m)?)?;
    m.add_class::<PyDetectionEvent>()?;
    m.add_class::<PyEventProducer>()?;
    m.add_class::<PyEventConsumer>()?;
    m.add("IpcError", py.get_type::<IpcError>())?;
    m.add(
        "InsecureDirectoryError",
        py.get_type::<InsecureDirectoryError>(),
    )?;
    m.add("InvalidEventError", py.get_type::<InvalidEventError>())?;
    m.add("ShutDownError", py.get_type::<ShutDownError>())?;
    m.add("SCHEMA_VERSION", core::SCHEMA_VERSION)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

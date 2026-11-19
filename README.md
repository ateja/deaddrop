# deaddrop

Local, near real time event exchange between endpoint applications on macOS and Windows, built as a single Rust core with Python and C/C++ bindings. The producer (a Python app) publishes events; native consumers (Rust or C++) receive them through an OS file system watcher. Neither side knows the other exists.

## Layout

```
crates/
  deaddrop/        Rust core: DetectionEvent, EventProducer, EventConsumer, file transport
  deaddrop-ffi/    C ABI (libdeaddrop .dylib/.dll/.so + static lib)
    include/deaddrop.h     C header
    include/deaddrop.hpp   header only C++17 RAII wrapper
    examples/consumer.cpp      C++ consumer agent
samples/
  python_publisher/    Python program that publishes detections
  cpp_listener/        C++ program (CMake) that listens and collects
  run_demo.sh          builds both and runs them together
  deaddrop-py/     Python wheel via PyO3 + maturin (pip install deaddrop-ipc)
.github/workflows/ci.yml       tests on macOS, Windows, Linux; builds wheels
```

## Try it

`samples/` has a Python publisher and a C++ listener that talk through deaddrop. Inside a Python virtualenv with maturin installed:

```bash
samples/run_demo.sh 5
```

See [samples/README.md](samples/README.md) for the two terminal walkthrough.

## How it works

```
Producer (Python)                          shared dir (producer writes, consumers read)
  publish_event(e)
   1. validate + serialize JSON
   2. create  .tmp-<nonce>.part            O_CREAT|O_EXCL  /  CREATE_NEW
   3. write, fsync
   4. rename -> evt-<seq:020>-<nonce>.json renamex_np(RENAME_EXCL) / MoveFileExW (no REPLACE)
   5. fsync directory                                 │
                                                      ▼ FSEvents / ReadDirectoryChangesW / inotify
Consumer (Rust / C++)                      watcher thread wakes
   6. list directory, pick unseen evt-*.json, skip temp and foreign files
   7. read (size capped), parse, validate, check seq_id matches file name
   8. sort batch by seq_id, call listeners
```

The OS notification is only a wakeup. The directory listing is the source of truth, so coalesced, duplicated or reordered notifications cannot cause missed or double deliveries. A periodic rescan (default 5 s) covers the case where the OS drops notifications entirely, which FSEvents can do under heavy load.

## Guarantees

| Property | How |
|---|---|
| No partial reads | Consumers only read `evt-*.json`, which only exist after an atomic rename of a fully written, fsynced file |
| No clobbering | Exclusive create for temp files; the rename refuses to replace an existing file; a 64 bit random nonce makes names unique even if a producer reuses a `seq_id` |
| No TOCTOU on the directory | The producer verifies ownership and mode after creating the directory, so losing a creation race to an attacker is detected |
| Ordering | Within a batch, listeners see events sorted by `seq_id` |
| At least once per consumer run | Every event file present while the consumer runs is delivered exactly once to that consumer instance. With `replay_existing = true` (default) a restarted consumer re-delivers files still on disk, so consumers should be idempotent on `seq_id` |
| Fan out | Any number of consumers can watch the same directory |
| Durability | With `durable = true` (default) an event that `publish_event` acknowledged survives power loss |
| Isolation | A panicking listener (or a C++ exception, or a Python exception) is caught and logged; delivery continues |

Not guaranteed: delivery of events the producer pruned before a consumer read them. Retention defaults to 1,024 files and 24 hours.

## Security model

- **Producer** creates the directory with restrictive permissions: Unix `0755` dirs and `0644` files by default (configurable, for example `0750`/`0640` plus a `group` to restrict readers). Windows uses a protected DACL: SYSTEM and Administrators full control, Authenticated Users read. Modes that grant group or other write are rejected at configuration time.
- **Consumer** refuses to start unless the directory is a real directory (not a symlink or junction), is not group/world writable (Unix), and is owned by a trusted account. Default trust is `Privileged`: root on Unix, SYSTEM or Administrators on Windows. That blocks directory squatting, where a local user pre-creates the path and feeds forged events.
- **Per file**: consumers ignore non regular files, cap reads at `max_event_bytes` (1 MiB), validate schema, and reject files whose `seq_id` does not match the name.
- **Known gap**: on Windows the consumer checks the directory owner, not the full DACL. Directories created by this library get a correct DACL; a directory provisioned by an installer must be locked down by that installer.

## Integration: producer team (Python)

```bash
pip install deaddrop-ipc            # wheel built from crates/deaddrop-py
```

```python
from deaddrop_ipc import EventProducer, DetectionEvent

# 1. Initialize with the agreed-upon secure directory path
producer = EventProducer.init("/var/run/vendor_ipc/events")

# 2. Construct and publish events (atomic write + rename, GIL released during I/O)
event = DetectionEvent(
    schema_version="1.0",
    seq_id=10492,
    timestamp_utc="2026-11-18T17:45:00Z",
    app_name="TargetApp",
    metadata={"vendor": "ExampleCorp", "confidence": 98},
)
producer.publish_event(event)

# 3. Release handles gracefully on application exit
producer.shutdown()
```

`schema_version` and `timestamp_utc` are optional (they default to `"1.0"` and now). `EventProducer` is also a context manager. Options: `durable`, `max_files`, `max_age_seconds`, `max_event_bytes`, `dir_mode`, `file_mode`, `group`. Errors raise `InvalidEventError`, `InsecureDirectoryError`, `ShutDownError` (all subclass `IpcError`). A Python `EventConsumer` with the same API as the Rust one is included for tests and tooling.

## Integration: consumer team (Rust)

```rust
use deaddrop::{EventConsumer, DetectionEvent, Result};
use std::path::PathBuf;

fn run_consumer() -> Result<()> {
    // 1. Initialize targeting the agreed-upon directory
    let mut consumer = EventConsumer::init(PathBuf::from("/var/run/vendor_ipc/events"))?;

    // 2. Register the handler, called on the watcher thread
    consumer.register_event_listener(|event: DetectionEvent| {
        println!("Received event {}: {}", event.seq_id, event.app_name);
    })?;

    // 3. Start the OS-native watcher (returns immediately)
    consumer.start_watching()?;

    // ... run the agent ...

    // 4. On application shutdown, stop the watcher and release handles
    consumer.shutdown()
}
```

Use `EventConsumer::with_config(ConsumerConfig { .. })` to set `trusted_owner`, `replay_existing`, `rescan_interval` and `max_event_bytes`.

## Integration: consumer team (C++)

```cpp
#include "deaddrop.hpp"

deaddrop::Consumer consumer("/var/run/vendor_ipc/events");
consumer.register_event_listener([](const deaddrop::DetectionEvent &e) {
    // e.seq_id, e.app_name, e.metadata_json (JSON object), e.event_json
});
consumer.start_watching();
// ...
consumer.shutdown();   // joins the watcher thread; the destructor does this too
```

Link with `-ldeaddrop`. For static linking, see the system libraries listed at the top of `deaddrop.h`. The plain C API (`dd_consumer_init`, `dd_consumer_register_listener`, ...) is in the same header.

## Deployment paths

| OS | Suggested directory | Note |
|---|---|---|
| macOS | `/Library/Application Support/<Vendor>/ipc/events` | `/var/run` is cleared at boot; fine if events only matter while both apps run |
| Windows | `C:\ProgramData\<Vendor>\ipc\events` | Producer service as LocalSystem creates it with the protected DACL |

Both sides must be configured with the same path. The producer must start (or the installer must create the directory) before a consumer starts watching; a consumer refuses a missing directory.

## Build and test

Requires Rust 1.91 or newer (set by the `notify` file watcher dependency). Check with `rustc --version`; upgrade with `rustup update stable`.

```bash
cargo test                                          # core + C ABI tests
cargo build --release -p deaddrop-ffi           # target/release/libdeaddrop.*
cd crates/deaddrop-py && maturin build --release   # wheel in target/wheels
cd crates/deaddrop-py && maturin develop && pytest tests
cargo run --release --example latency -p deaddrop -- 2000
```

## Measured (Linux container, overlay fs, inotify; 3,000 samples)

| | p50 | p95 | p99 |
|---|---|---|---|
| `publish_event`, fsync on | 0.96 ms | 1.46 ms | 8.6 ms |
| `publish_event`, fsync off | 0.49 ms | 0.73 ms | 3.0 ms |
| publish to callback, fsync on | 1.30 ms | 2.12 ms | 2.5 ms |

Treat these as a sanity check, not a macOS or Windows number. FSEvents and `ReadDirectoryChangesW` have different latency profiles; run the `latency` example on target hardware (CI does on each push).

## Differences from the proposal

- `start_watching()` is non blocking: it spawns the watcher thread and returns. The proposal's step 4 (`shutdown`) only makes sense that way.
- `publish_event` is synchronous, not fire and forget. It returns once the event is durable and visible, typically about 1 ms. Making it async would mean an unacknowledged in-memory queue that can lose events on crash.
- Added: retention (the proposal did not say who deletes files; consumers are read only, so the producer prunes), directory trust checks on the consumer, a periodic safety rescan, size limits, and `seq_id`/file name cross checks.
- The doc's code samples had small errors, fixed here: `rom endpoint_ipc import` (missing `f`; the library was then called endpoint-ipc), the Rust consumer was missing `Ok(())`/closing brace, and two sections were numbered 2.

## Swapping the transport

`EventProducer::with_transport` and `EventConsumer::with_transport` accept any `ProducerTransport` / `ConsumerTransport`. A named pipe or Unix socket backend can be added without changing producer or consumer call sites.

## Known limitations and next steps

- Each wakeup lists the whole directory: O(files on disk), bounded by retention at about 1,100 files. If retention is raised a lot, switch to the paths carried in the notification and keep full scans for the periodic rescan.
- Windows DACL inspection on the consumer side (owner only today).
- Only the Linux paths were executed in this build environment. The macOS and Windows code type-checks and lints clean on both targets, and the CI workflow runs the full suite on real macOS and Windows runners.

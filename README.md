# deaddrop

Local, near real time event exchange between endpoint applications on macOS, Windows and Linux, built as a single Rust core with Python and C/C++ bindings. The producer (a Python app) publishes events; native consumers (Rust or C++) receive them. Neither side knows the other exists.

Two transports carry the events, behind one API. A YAML file picks one at runtime, with no code changes:

- **file** (default): each event is a file in a shared folder, written atomically and picked up by the OS file watcher. Durable and fan-out friendly.
- **ipc**: events go straight to the consumer over the OS's own IPC: named pipes on Windows, XPC on macOS, Unix domain sockets on Linux. Nothing touches disk, and delivery measured about 30 times faster than the file transport on Linux and 170 times faster on Windows.

## Layout

```
crates/
  deaddrop/          Rust core: DetectionEvent, EventProducer, EventConsumer
    src/config.rs      YAML config: picks the transport at runtime
    src/file/          file transport (atomic write + rename, OS file watcher)
    src/ipc/           ipc transport: named_pipe.rs (Windows), xpc.rs (macOS), unix_socket.rs (Linux)
    src/logging.rs     opt-in stderr logging (DEADDROP_LOG)
  deaddrop-ffi/      C ABI (libdeaddrop .dylib/.dll/.so + static lib)
    include/deaddrop.h     C header
    include/deaddrop.hpp   header only C++17 RAII wrapper
    examples/consumer.cpp  C++ consumer agent
  deaddrop-py/       Python package deaddrop-ipc via PyO3 + maturin
samples/
  deaddrop.yaml        transport: file | ipc, for the samples
  python_publisher/    Python program that publishes events
  cpp_listener/        C++ program (CMake) that listens and collects
  run_demo.sh          builds both and runs them together, with either transport
.github/workflows/ci.yml   tests and both demos on macOS, Windows, Linux; builds wheels
```

## Run the samples (from a fresh clone)

`samples/` has a Python publisher and a C++ listener that talk only through deaddrop. These steps take you from nothing to both programs running. The Python side lives in a virtual environment (venv) inside the repo, so nothing is installed system wide.

### 1. Install the prerequisites (once per machine)

| Tool | Version | macOS | Windows | Linux (Debian/Ubuntu) |
|---|---|---|---|---|
| Git | any | `xcode-select --install` | https://git-scm.com (includes Git Bash) | `sudo apt install git` |
| Rust | 1.91+ | https://rustup.rs | https://rustup.rs | https://rustup.rs |
| Python | 3.9+ | https://python.org or `brew install python` | https://python.org (tick **Add python.exe to PATH**) | `sudo apt install python3 python3-venv` |
| CMake | 3.18+ | `brew install cmake` | https://cmake.org/download | `sudo apt install cmake` |
| C++17 compiler | | `xcode-select --install` | Visual Studio Build Tools, workload **Desktop development with C++** | `sudo apt install build-essential` |

Check Rust with `rustc --version`. If it is older than 1.91, run `rustup update stable`.

### 2. Clone the repository

```bash
git clone https://github.com/ateja/deaddrop.git
```

```bash
cd deaddrop
```

### 3. Create a virtual environment and install the Python bindings into it

macOS / Linux:

```bash
python3 -m venv .venv
```

```bash
source .venv/bin/activate
```

```bash
python -m pip install --upgrade pip
```

```bash
pip install ./crates/deaddrop-py
```

Windows (PowerShell):

```powershell
py -m venv .venv
```

```powershell
.venv\Scripts\Activate.ps1
```

```powershell
python -m pip install --upgrade pip
```

```powershell
pip install ./crates/deaddrop-py
```

`pip install ./crates/deaddrop-py` compiles the Rust core and installs the `deaddrop_ipc` package into `.venv` (about 30 seconds the first time). Your prompt now starts with `(.venv)`. Activate the venv again in every new terminal where you run Python (step 3's second command), and run `deactivate` to leave it. `.venv/` is git-ignored.

Check it worked:

```bash
python -c "import deaddrop_ipc; print(deaddrop_ipc.__version__)"
```

### 4. Build the C library and the C++ listener

The same commands work on every OS:

```bash
cargo build --release -p deaddrop-ffi
```

```bash
cmake -S samples/cpp_listener -B samples/cpp_listener/build -DCMAKE_BUILD_TYPE=Release
```

```bash
cmake --build samples/cpp_listener/build --config Release
```

The listener ends up at `samples/cpp_listener/build/listener` (macOS/Linux) or `samples\cpp_listener\build\Release\listener.exe` (Windows).

### 5. Run them

Open two terminals in the `deaddrop` folder.

**Terminal A, the C++ listener** (runs until Ctrl+C; add `--count 10` to stop after 10 events):

```bash
samples/cpp_listener/build/listener
```

On Windows:

```powershell
samples\cpp_listener\build\Release\listener.exe
```

**Terminal B, the Python publisher** (activate the venv first, as in step 3):

```bash
python samples/python_publisher/publisher.py --count 10 --interval 0.5
```

Each `sent` line in terminal B shows up as a `recv` line in terminal A. When the listener stops, it prints a summary. Both programs default to the same folder, `<temp>/deaddrop-demo/events`, so no flags are needed, and they can start in either order.

**Or both in one command** (macOS/Linux, or Git Bash on Windows, with the venv active):

```bash
samples/run_demo.sh 5
```

It rebuilds anything missing, runs both programs, and ends with `==> demo OK (file): listener received all 5 events`. On Windows, run it in **Git Bash**, not WSL: Git Bash runs the Windows build (and named pipes), while WSL is Linux and would test the Linux build instead.

**Choosing the transport.** Both ways above use the file transport. To switch:

- `run_demo.sh`: set `transport: ipc` in [samples/deaddrop.yaml](samples/deaddrop.yaml), or override it for one run with `samples/run_demo.sh 5 ipc`. The first line of output says which transport and endpoint it used, for example `==> transport: ipc (named pipe \\.\pipe\com.deaddrop.demo)`.
- Two terminals: add `--config samples/deaddrop.yaml` to both commands (or set `DEADDROP_CONFIG` to that file's full path in both terminals).
- On macOS, an IPC listener must be started by launchd (see [macOS: the consumer runs under launchd](#macos-the-consumer-runs-under-launchd)). `run_demo.sh` registers it for you, so use the script there; a listener started by hand fails with an error explaining this.

[samples/README.md](samples/README.md) shows sample output and every command line option.

### Troubleshooting

| Symptom | Fix |
|---|---|
| `rustc 1.xx is not supported by the following packages` | Rust is older than 1.91: `rustup update stable` |
| `ModuleNotFoundError: No module named 'deaddrop_ipc'` | The venv is not active in this terminal (step 3, second command), or `pip install ./crates/deaddrop-py` was skipped |
| Windows: `running scripts is disabled on this system` when activating | Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once, then activate again |
| Windows: CMake `No CMAKE_CXX_COMPILER could be found` | Install Visual Studio Build Tools with **Desktop development with C++**, then open a new terminal |
| `libdeaddrop not found` / `Missing ...deaddrop.dll.lib` from CMake | Run `cargo build --release -p deaddrop-ffi` first (step 4) |
| Listener: `deaddrop error 4: insecure IPC directory ... lets group or others write` | Another tool loosened the folder's permissions. Delete `<temp>/deaddrop-demo` and let the publisher recreate it |
| Listener stays on `waiting for ... to be created` | (file) The publisher is using a different folder. Pass the same `--dir PATH` to both |
| Listener: `cannot listen on XPC service ... registered with launchd` | (ipc, macOS) The listener was started by hand. Use `samples/run_demo.sh 5 ipc`, or run it from a launchd plist |
| Listener: `cannot create \\.\pipe\... (another consumer may already own this name)` | (ipc, Windows) Another listener is still running. Close it, or end `listener.exe` in Task Manager |
| Listener: `another consumer is already listening on /tmp/....sock` | (ipc, Linux) Another listener is still running; stop it first |
| Publisher prints `sent` but the listener shows nothing | (ipc) The listener is not running or uses another `ipc.name`; events wait in the publisher's memory until it connects. Run both with `--debug` to see the connection attempts |
| Windows: the demo behaves like Linux (Unix socket, inotify) | You are in WSL. Open **Git Bash** from the Start menu instead; `uname -s` should print `MINGW64_...` |

## Transports: file or IPC

Both transports are fully supported on all three operating systems and sit behind the same `EventProducer` / `EventConsumer` API (Rust, Python, C and C++). Which one runs is decided at runtime by a YAML file; producer and consumer code does not change.

| | `file` (default) | `ipc` |
|---|---|---|
| Windows | shared folder + `ReadDirectoryChangesW` | named pipe `\\.\pipe\<name>` |
| macOS | shared folder + FSEvents | XPC Mach service `<name>` |
| Linux | shared folder + inotify | Unix socket `/tmp/<name>.sock` |
| Events survive a producer crash or restart | yes (fsynced files) | no: undelivered events live in the producer's memory |
| Consumer can start later and catch up | yes (`replay_existing`) | only events still queued in a running producer (`ipc.max_pending`, default 1,024) |
| Consumers per producer | any number | one per endpoint name (a second one is refused) |
| Touches disk | yes | no |
| `publish_event` returns after, p50 (Linux, measured) | 0.84 ms (fsync) | 0.009 ms (queued) |
| Publish to callback, p50 / p95 (Linux, measured) | 1.23 / 2.12 ms | 0.043 / 0.057 ms |
| Publish to callback, p50 / p95 (Windows CI runner, measured) | 8.44 / 12.04 ms | 0.050 / 0.067 ms |
| macOS setup | none | the consumer must run under launchd (see below) |

### Choosing at runtime

`init(path)` keeps its signature. It looks for a config in this order:

1. `path` itself, if it is a `.yaml` / `.yml` file.
2. The file named by the `DEADDROP_CONFIG` environment variable; `path` is then the default folder for the file transport.
3. Neither: the file transport in `path`, exactly as before configs existed.

```yaml
transport: ipc            # file | ipc
file:
  dir: /var/run/vendor_ipc/events   # optional when init() is given a folder
ipc:
  name: com.example.deaddrop        # pipe / XPC service / socket name, same on both sides
  trusted_peer: privileged          # privileged | current_user | any
  max_pending: 1024                 # producer queue while the consumer is unreachable
  flush_timeout_ms: 5000            # how long shutdown() keeps delivering
```

Unknown keys and values are errors, so a typo never silently falls back to the other transport. `endpoint()` (Rust), `.endpoint` (Python), `dd_producer_endpoint()` / `endpoint()` (C/C++) report what was chosen, e.g. `ipc: named pipe \\.\pipe\com.example.deaddrop`. [samples/deaddrop.yaml](samples/deaddrop.yaml) is a commented example.

## How the file transport works

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

## How the IPC transport works

The consumer listens; the producer connects. `publish_event` puts the event in a bounded in-memory queue and returns; a delivery thread sends it, connecting and reconnecting with backoff (50 ms up to 1 s), so a producer may start before its consumer. Events arrive in publish order. `shutdown()` waits up to `flush_timeout_ms` for the queue to drain, then drops what is left with a warning.

- **Named pipes (Windows):** byte mode, inbound only, `PIPE_REJECT_REMOTE_CLIENTS`, and `FILE_FLAG_FIRST_PIPE_INSTANCE` so no other process can take the name while the consumer runs. Frames are a `DDP1` handshake, then length-prefixed JSON.
- **XPC (macOS):** each event is an XPC dictionary sent with a synchronous reply, so a send only counts once the consumer has it.
- **Unix sockets (Linux):** same framing as pipes; the socket is `0600` unless the policy needs other accounts.

Both sides check who is on the other end before any event crosses: the consumer checks the producer and the producer checks the consumer, using the same policies as the file transport's folder checks (`privileged` means root, SYSTEM or Administrators). The OS supplies the identity: `SO_PEERCRED` on Linux, the XPC peer's euid on macOS, the client process token and the pipe's owner on Windows. A consumer's policy comes from `ipc.trusted_peer` if set, else from its code (`trusted_owner`); a producer's from `ipc.trusted_peer`, else `privileged`.

### macOS: the consumer runs under launchd

macOS only lets a process listen on an XPC Mach service that launchd registered for it. Measured in CI: a listener started any other way fails at once with `Connection invalid`, and deaddrop turns that into an error naming the fix. In production the consumer is a LaunchDaemon (or LaunchAgent) whose plist declares the service:

```xml
<key>Label</key><string>com.example.agent</string>
<key>ProgramArguments</key><array><string>/usr/local/bin/agent</string></array>
<key>MachServices</key><dict><key>com.example.deaddrop</key><true/></dict>
<key>EnvironmentVariables</key><dict><key>DEADDROP_CONFIG</key><string>/etc/example/deaddrop.yaml</string></dict>
```

launchd then also starts the consumer on demand when a producer connects. `samples/run_demo.sh` writes and loads a plist like this for the length of the demo, and removes it afterwards. Producers need nothing special.

## Guarantees

### File transport

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

### IPC transport

| Property | How |
|---|---|
| No partial messages | Named pipes and Unix sockets carry length-prefixed frames after a `DDP1` handshake; XPC carries whole dictionaries. A pipe or socket frame over `max_event_bytes` closes that connection; an oversized XPC message is ignored |
| Ordering | Events from one producer arrive in publish order: one delivery thread per producer, one reader per connection, one dispatcher thread per consumer |
| Delivery | At least once while both sides run. A send that fails is retried after reconnecting, so a consumer may see an event twice; be idempotent on `seq_id`, as with the file transport. XPC sends wait for the consumer's reply; pipe and socket sends count as delivered once the OS accepted the bytes |
| Not durable | Undelivered events live in the producer's memory. They are lost if the producer process dies, if the queue overflows `ipc.max_pending` (oldest dropped first, with a warning), or if the consumer is still unreachable when `shutdown()`'s `ipc.flush_timeout_ms` runs out |
| No back pressure | `publish_event` never blocks on the consumer; it only queues |
| One consumer per endpoint | A second consumer on the same `ipc.name` is refused at `start_watching()` |
| Isolation | Same as the file transport: a failing listener is caught and logged; delivery continues |

## Security model

### File transport

- **Producer** creates the directory with restrictive permissions: Unix `0755` dirs and `0644` files by default (configurable, for example `0750`/`0640` plus a `group` to restrict readers). Windows uses a protected DACL: SYSTEM and Administrators full control, Authenticated Users read. Modes that grant group or other write are rejected at configuration time.
- **Consumer** refuses to start unless the directory is a real directory (not a symlink or junction), is not group/world writable (Unix), and is owned by a trusted account. Default trust is `Privileged`: root on Unix, SYSTEM or Administrators on Windows. That blocks directory squatting, where a local user pre-creates the path and feeds forged events.
- **Per file**: consumers ignore non regular files, cap reads at `max_event_bytes` (1 MiB), validate schema, and reject files whose `seq_id` does not match the name.
- **Known gap**: on Windows the consumer checks the directory owner, not the full DACL. Directories created by this library get a correct DACL; a directory provisioned by an installer must be locked down by that installer.

### IPC transport

- **Peer identity, both ways.** Before any event crosses, the consumer checks the account of the producer process and the producer checks the account of the consumer process, with the same `privileged` / `current_user` / `any` policies as the folder checks. The OS supplies the identity: `SO_PEERCRED` on Linux, the XPC peer's euid on macOS, and on Windows the client process token (consumer side) and the pipe's owner (producer side).
- **No name squatting.** Windows pipes are created with `FILE_FLAG_FIRST_PIPE_INSTANCE`, so no other process can take the name while the consumer runs, and with `PIPE_REJECT_REMOTE_CLIENTS`. Producers open the pipe at identification level, so the consumer cannot impersonate them. On macOS only the launchd job that declares the service can listen on it. On Linux the consumer refuses a path that exists but is not a socket, a socket another consumer is listening on, or a stale socket owned by another account; it creates its socket `0600` unless the policy needs other accounts.
- **Who may connect.** The Windows pipe's default DACL only lets SYSTEM, Administrators and the consumer's own account write (`trusted_peer: any` widens it to authenticated users).

### Both

- **Protect the config file.** Whoever can edit the YAML can switch the transport or loosen `trusted_peer`. Install it writable only by root, SYSTEM or Administrators; the library does not check the config file's own permissions.

## Integration: producer team (Python)

```bash
pip install ./crates/deaddrop-py     # from a clone, inside a venv; not on PyPI yet
```

```python
from deaddrop_ipc import EventProducer, DetectionEvent

# 1. Initialize: a shared folder (file transport), or a deaddrop.yaml that
#    selects file or ipc. With DEADDROP_CONFIG set, the folder form also
#    follows that config.
producer = EventProducer.init("/var/run/vendor_ipc/events")
print(producer.endpoint)   # e.g. "file: /var/run/vendor_ipc/events" or "ipc: XPC service com.example.deaddrop"

# 2. Construct and publish events
#    file: atomic write + rename before it returns; ipc: queued for the delivery thread
#    (the GIL is released during I/O either way)
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

`schema_version` and `timestamp_utc` are optional (they default to `"1.0"` and now). `EventProducer` is also a context manager. Keyword options `durable`, `max_files`, `max_age_seconds`, `max_event_bytes`, `dir_mode`, `file_mode` and `group` tune the file transport and are ignored when the config selects `ipc` (the `ipc:` section of the YAML tunes that one). Errors raise `InvalidEventError`, `InsecureDirectoryError`, `ShutDownError` or, for IPC failures, `IpcError` itself (the others subclass it). A Python `EventConsumer` with the same API as the Rust one is included for tests and tooling.

## Integration: consumer team (Rust)

```rust
use deaddrop::{EventConsumer, DetectionEvent, Result};
use std::path::PathBuf;

fn run_consumer() -> Result<()> {
    // 1. Initialize: a shared folder (file transport), or a deaddrop.yaml
    //    that selects file or ipc (DEADDROP_CONFIG works too)
    let mut consumer = EventConsumer::init(PathBuf::from("/var/run/vendor_ipc/events"))?;
    println!("receiving via {}", consumer.endpoint());

    // 2. Register the handler, called on the transport's thread
    consumer.register_event_listener(|event: DetectionEvent| {
        println!("Received event {}: {}", event.seq_id, event.app_name);
    })?;

    // 3. Start the file watcher or the IPC listener (returns immediately)
    consumer.start_watching()?;

    // ... run the agent ...

    // 4. On application shutdown, stop the watcher and release handles
    consumer.shutdown()
}
```

To set `trusted_owner`, `replay_existing`, `rescan_interval` or `max_event_bytes` while still honouring the YAML, use `EventConsumer::init_with(target, |cfg| cfg.trusted_owner = TrustedOwner::Privileged)`. With IPC, `trusted_owner` is the policy for the connecting producer (unless the YAML sets `ipc.trusted_peer`) and `max_event_bytes` still applies; the folder options are unused. `EventConsumer::with_config` always builds the file transport, and `EventProducer::init_with` does the same job on the producer side.

## Integration: consumer team (C++)

```cpp
#include "deaddrop.hpp"

// A shared folder (file transport), or a deaddrop.yaml that selects file or ipc.
deaddrop::Consumer consumer("/var/run/vendor_ipc/events");
std::printf("receiving via %s\n", consumer.endpoint().c_str());
consumer.register_event_listener([](const deaddrop::DetectionEvent &e) {
    // e.seq_id, e.app_name, e.metadata_json (JSON object), e.event_json
});
consumer.start_watching();
// ...
consumer.shutdown();   // joins the transport's thread; the destructor does this too
```

Link with `-ldeaddrop`. For static linking, see the system libraries listed at the top of `deaddrop.h`. The plain C API (`dd_consumer_init`, `dd_consumer_register_listener`, `dd_consumer_endpoint`, ...) is in the same header. IPC failures return `DD_ERR_IPC`.

## Deployment

Both sides must use the same transport, and the same folder (file) or `ipc.name` (ipc). Ship one config file and point both programs at it with `DEADDROP_CONFIG` (or pass its path to `init()`).

| OS | Config file | Shared folder (file) | IPC endpoint | How the programs get `DEADDROP_CONFIG` |
|---|---|---|---|---|
| macOS | `/Library/Application Support/<Vendor>/deaddrop.yaml` | `/Library/Application Support/<Vendor>/ipc/events` | XPC service `com.<vendor>.deaddrop` | `EnvironmentVariables` in each launchd plist; the consumer's plist also lists the service under `MachServices` |
| Windows | `C:\ProgramData\<Vendor>\deaddrop.yaml` | `C:\ProgramData\<Vendor>\ipc\events` | `\\.\pipe\com.<vendor>.deaddrop` | the services' environment, or system wide with `setx /M DEADDROP_CONFIG <path>` |
| Linux | `/etc/<vendor>/deaddrop.yaml` | `/var/lib/<vendor>/events` | `/tmp/com.<vendor>.deaddrop.sock` | `Environment=` in the systemd units |

Install the config writable only by root, SYSTEM or Administrators (see [Security model](#security-model)) and set `ipc.trusted_peer: privileged`.

- **file:** the producer must start first, or the installer must create the folder; a consumer refuses a missing folder. On macOS, `/var/run` is cleared at boot, which is fine only if events matter just while both apps run. On Windows, a producer running as LocalSystem creates the folder with the protected DACL.
- **ipc:** either side may start first; the producer holds events until the consumer is listening. On macOS, launchd starts the consumer when a producer first connects if its plist declares the service.

## Build and test

Requires Rust 1.91 or newer (set by the `notify` file watcher dependency). Check with `rustc --version`; upgrade with `rustup update stable`.

```bash
cargo test                                          # core (file + ipc) + C ABI tests
cargo build --release -p deaddrop-ffi           # target/release/libdeaddrop.*
cd crates/deaddrop-py && maturin build --release   # wheel in target/wheels
cd crates/deaddrop-py && maturin develop && pytest tests
cargo run --release --example latency -p deaddrop -- 2000         # file transport
cargo run --release --example latency -p deaddrop -- 2000 --ipc   # ipc (not on macOS: needs launchd)
```

`cargo test` runs the IPC tests on Linux and Windows. On macOS they are skipped, because an XPC listener cannot run under `cargo test` without launchd; a macOS-only test checks the explanatory error instead, and CI covers XPC end to end with `samples/run_demo.sh 5 ipc`, which runs the listener under launchd.

## Diagnostic logging

The library logs each step of an exchange through the Rust `log` crate, under two targets, `deaddrop::producer` and `deaddrop::consumer`. At `debug` level that covers:

- **Both transports:** which config file was read and which transport and endpoint it selected; each event received and delivered to listeners.
- **file:** which directory and temp file the producer writes, the flush to disk, the atomic rename and the final file name; on the consumer side, the watcher backend in use, each OS notification, in-flight temp files it skips, and which file it reads.
- **ipc:** each event queued and delivered, with timings; connection attempts while the consumer is unreachable, reconnects, and who is on the other end (process id, plus the account on Linux and macOS); the shutdown flush.

| Where | How to turn it on | Output |
|---|---|---|
| Python | `DEADDROP_LOG=debug` in the environment, or `deaddrop_ipc.set_log_level("debug")` | stderr |
| C / C++ | `DEADDROP_LOG=debug`, or `dd_set_log_level("debug")` / `deaddrop::set_log_level("debug")` | stderr |
| Rust | `deaddrop::logging::init_from_env()`, or your own logger (`env_logger`, ...) | your logger |

Levels: `off`, `error`, `warn` (default), `info`, `debug`, `trace` (adds data-write and close notifications). Every line starts with `[deaddrop:producer]` or `[deaddrop:consumer]`, then the time, level and process id:

```
[deaddrop:producer] 16:12:20.799 DEBUG pid=2342 seq=7 atomic rename .tmp-6d2c6f110d4a4efe.part -> evt-00000000000000000007-e596d224d3c7bdad.json
```

`samples/run_demo.sh` turns this on and colour codes it; see [Behind the scenes](samples/README.md#behind-the-scenes).

## Measured

Publish to callback, through the real OS mechanism, from the `latency` example (`--ipc` for the IPC transport):

| Where | Samples | file p50 / p95 | ipc p50 / p95 |
|---|---|---|---|
| Linux (build container, inotify / Unix socket) | 3,000 | 1.23 / 2.12 ms | 0.043 / 0.057 ms |
| Windows (CI runner, `ReadDirectoryChangesW` / named pipe) | 1,000 | 8.44 / 12.04 ms | 0.050 / 0.067 ms |
| macOS (CI runner, FSEvents / XPC) | 1,000 | 13.3 / 16.9 ms | not benchmarked; each XPC send took 0.09 to 0.31 ms in the demo logs |

`publish_event` itself returns after 0.84 ms (file, with fsync) or 0.009 ms (ipc, queued) at p50 on Linux. Where the file transport's time goes differs by OS: on Windows it is mostly the flush to disk, on macOS mostly FSEvents batching notifications. CI runners are shared virtual machines, so treat these numbers as an upper bound and run the `latency` example on your own hardware.

## Differences from the proposal

- `start_watching()` is non blocking: it starts the file watcher or the IPC listener on a background thread and returns. The proposal's step 4 (`shutdown`) only makes sense that way.
- The proposal planned to swap the file backend for named pipes or AF_UNIX sockets later "with zero API changes". That is now the `ipc` transport, chosen per deployment in YAML, with XPC instead of AF_UNIX on macOS, where XPC is the native mechanism.
- `publish_event` with the **file** transport is synchronous, not fire and forget: it returns once the event is durable and visible, typically about 1 ms. With **ipc** it is non blocking, as the proposal described, at the cost of losing queued events if the producer dies.
- Added: retention (the proposal did not say who deletes files; consumers are read only, so the producer prunes), directory trust checks on the consumer, a periodic safety rescan, size limits, and `seq_id`/file name cross checks.
- The doc's code samples had small errors, fixed here: `rom endpoint_ipc import` (missing `f`; the library was then called endpoint-ipc), the Rust consumer was missing `Ok(())`/closing brace, and two sections were numbered 2.

## Swapping the transport

The YAML config switches between the two built-in transports. For anything else, `EventProducer::with_transport` and `EventConsumer::with_transport` accept any `ProducerTransport` / `ConsumerTransport` implementation, again without changing producer or consumer call sites.

## Known limitations and next steps

- **file:** each wakeup lists the whole directory: O(files on disk), bounded by retention at about 1,100 files. If retention is raised a lot, switch to the paths carried in the notification and keep full scans for the periodic rescan.
- **file:** on Windows the consumer checks the folder's owner, not its full DACL.
- **ipc:** one consumer per endpoint name, and no persistence: events queued in a producer that dies are lost. Use the file transport where either matters.
- **ipc, macOS:** the consumer must run under launchd, which also means its IPC path cannot be exercised by `cargo test`; CI covers it through the launchd demo.
- **ipc, Linux:** sockets live in `/tmp`; a configurable socket directory would suit hardened systems better.
- **Both:** the library does not check the permissions of the config file itself.

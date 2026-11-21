# Samples: Python publisher → C++ listener

Two small programs that talk only through deaddrop:

| Program | Role | Prints |
|---|---|---|
| `python_publisher/publisher.py` | Publishes simulated events from a set of fictional apps (Contoso Mail, Fabrikam Notes, ...) | `[publisher] sent seq=... app=... metadata=...` for each event |
| `cpp_listener/listener.cpp` | Listens, prints and collects every event; prints a per app summary on exit | `[listener] recv #n seq=... app=... metadata=...`, then a summary |

Neither program knows about the other. They only agree on where events go: a shared folder (file transport) or an IPC endpoint name (ipc transport), set in [`deaddrop.yaml`](deaddrop.yaml).

## File or IPC

`run_demo.sh` reads [`samples/deaddrop.yaml`](deaddrop.yaml). Change `transport: file` to `transport: ipc` and run it again, or override it for one run:

```bash
samples/run_demo.sh 5 ipc
```

| `transport` | Windows | macOS | Linux |
|---|---|---|---|
| `file` | shared folder | shared folder | shared folder |
| `ipc` | named pipe `\\.\pipe\com.deaddrop.demo` | XPC service `com.deaddrop.demo` | Unix socket `/tmp/com.deaddrop.demo.sock` |

With `ipc` on macOS, the script registers the listener as a temporary launchd agent, because macOS only lets launchd-registered processes listen on an XPC service. It prints `(listener started by launchd as gui/501/...)` and removes the agent when the run ends.

To run the programs by hand with a config, pass `--config samples/deaddrop.yaml` to both, or `export DEADDROP_CONFIG=$PWD/samples/deaddrop.yaml`. On macOS the IPC listener must be started by launchd; see the main README.

## Setup

Follow [Run the samples](../README.md#run-the-samples-from-a-fresh-clone) in the main README: install the prerequisites, clone, create the virtual environment, build, run. Short version once everything is installed, from the repository root with the venv active:

```bash
samples/run_demo.sh 5
```

## Expected output

Abridged, from `samples/run_demo.sh 5`:

```
[listener] waiting for /tmp/deaddrop-demo-1234/events to be created by the publisher...
[publisher] writing to /tmp/deaddrop-demo-1234/events
[publisher] sent seq=1795069270002 app='Litware Viewer' at=2026-11-19T06:21:10.004Z metadata={"confidence": 98, ...}
[listener] watching /tmp/deaddrop-demo-1234/events (Ctrl+C to stop)
[listener] recv #1 seq=1795069270002 app='Litware Viewer' at=2026-11-19T06:21:10.004Z metadata={"confidence":98,...}
[listener] recv #2 seq=1795069270003 app='Contoso Mail' at=2026-11-19T06:21:10.206Z metadata={"confidence":91,...}
[publisher] sent seq=1795069270003 app='Contoso Mail' at=2026-11-19T06:21:10.206Z metadata={"confidence": 91, ...}
...
[listener] collected 5 event(s)
[listener]   seq range: 1795069270002 .. 1795069270006
[listener]   Contoso Mail       3
[listener]   Litware Viewer     2
==> demo OK: listener received all 5 events
```

The listener often prints `recv` before the publisher prints `sent`: delivery finishes before Python gets back to its `print`.

## Two terminals

```bash
samples/cpp_listener/build/listener
```

```bash
python samples/python_publisher/publisher.py --count 10 --interval 0.5
```

Run the first in one terminal and the second in another (with the venv active). On Windows the listener is `samples\cpp_listener\build\Release\listener.exe`. Both default to `<temp>/deaddrop-demo/events`, so no flags are needed, and they can start in either order: the listener waits for the folder and by default also delivers events already on disk.

## Options

`publisher.py`

| Flag | Default | Meaning |
|---|---|---|
| `--config FILE` | `DEADDROP_CONFIG` | deaddrop YAML selecting file or ipc |
| `--dir PATH` | `<temp>/deaddrop-demo/events` | shared directory (file transport) |
| `--count N` | 10 | events to send; `0` = until Ctrl+C |
| `--interval S` | 0.5 | seconds between events |
| `--start-seq N` | current Unix time in ms | first `seq_id` (monotonic across runs) |
| `--seed N` | random | repeatable app choices |
| `--debug` | off | print the library's internal steps to stderr (same as `DEADDROP_LOG=debug`) |

`listener`

| Flag | Default | Meaning |
|---|---|---|
| `--config FILE` | `DEADDROP_CONFIG` | deaddrop YAML selecting file or ipc |
| `--dir PATH` | `<temp>/deaddrop-demo/events` | shared directory (file transport) |
| `--count N` | unlimited | exit after N events |
| `--timeout S` | none | exit after S seconds |
| `--trust MODE` | `current-user` | who the producer must run as (folder owner, or IPC peer): `privileged`, `current-user`, `any`. `ipc.trusted_peer` in the YAML overrides it |
| `--new-only` | off | ignore events already on disk |
| `--debug` | off | print the library's internal steps to stderr (same as `DEADDROP_LOG=debug`) |

Listener exit codes: `0` success, `1` error (for example an insecure directory), `2` `--count` not reached before `--timeout`.

`--trust current-user` suits a demo, where you create the folder under your own account. A production consumer should keep the library default, `privileged`, so only a root/SYSTEM-owned directory is trusted.

## Behind the scenes

`run_demo.sh` runs both programs with `DEADDROP_LOG=debug`, so the library reports every step it takes. Those lines are indented and coloured, separate from what the programs print themselves:

- **yellow** `| [deaddrop:producer] ...`: inside the Python publisher
- **cyan** `| [deaddrop:consumer] ...`: inside the C++ listener

One event, start to finish, with the **file** transport (abridged):

```
      | [deaddrop:producer] DEBUG seq=…596 writing 217 bytes to temp file /tmp/deaddrop-demo-2336/events/.tmp-6d2c6f110d4a4efe.part (hidden from consumers until renamed)
      | [deaddrop:consumer] DEBUG OS notification Create(File): .tmp-6d2c6f110d4a4efe.part
      | [deaddrop:consumer] DEBUG scan after OS notification: ignoring .tmp-6d2c6f110d4a4efe.part (producer is still writing it; it becomes visible only after the atomic rename)
      | [deaddrop:producer] DEBUG seq=…596 temp file complete and flushed to disk (fsync) in 1.01 ms
      | [deaddrop:producer] DEBUG seq=…596 atomic rename .tmp-6d2c6f110d4a4efe.part -> evt-00000001795191140596-e596d224d3c7bdad.json
      | [deaddrop:consumer] DEBUG OS notification Modify(Name(Both)): .tmp-6d2c6f110d4a4efe.part -> evt-00000001795191140596-e596d224d3c7bdad.json
      | [deaddrop:consumer] DEBUG scan after OS notification: reading new event file /tmp/deaddrop-demo-2336/events/evt-00000001795191140596-e596d224d3c7bdad.json
      | [deaddrop:consumer] DEBUG received event seq=…596 app="Adatum Reports" from evt-00000001795191140596-e596d224d3c7bdad.json
      | [deaddrop:consumer] DEBUG delivering seq=…596 to 1 listener(s)
[listener] recv #2 seq=…596 app='Adatum Reports' ...
      | [deaddrop:producer] DEBUG seq=…596 rename done in 0.04 ms: event is now visible to consumers as /tmp/deaddrop-demo-2336/events/evt-00000001795191140596-e596d224d3c7bdad.json (publish took 1.40 ms)
[publisher] sent seq=…596 app='Adatum Reports' ...
```

And with the **ipc** transport (`samples/run_demo.sh 5 ipc` on Linux; on Windows the endpoint is `named pipe \\.\pipe\com.deaddrop.demo`, on macOS `XPC service com.deaddrop.demo`):

```
      | [deaddrop:consumer] DEBUG config /tmp/deaddrop-demo-18886/deaddrop.yaml: transport=ipc (Unix socket /tmp/com.deaddrop.demo.sock)
      | [deaddrop:consumer] DEBUG IPC transport: listening on Unix socket /tmp/com.deaddrop.demo.sock (producers must be CurrentUser)
[listener] receiving via ipc: Unix socket /tmp/com.deaddrop.demo.sock (Ctrl+C to stop)
      | [deaddrop:producer] DEBUG IPC transport: will deliver to Unix socket /tmp/com.deaddrop.demo.sock (consumer must be CurrentUser; queue up to 1024 events while it is unreachable)
[publisher] sending via ipc: Unix socket /tmp/com.deaddrop.demo.sock
      | [deaddrop:producer] DEBUG seq=…197 queued 217 bytes for Unix socket /tmp/com.deaddrop.demo.sock (1 pending)
      | [deaddrop:producer] DEBUG connected to Unix socket /tmp/com.deaddrop.demo.sock (consumer pid 18894 uid 0)
      | [deaddrop:producer] DEBUG seq=…197 delivered 217 bytes to Unix socket /tmp/com.deaddrop.demo.sock in 0.00 ms (queued 0.09 ms before that)
      | [deaddrop:consumer] DEBUG accepted connection from producer pid 18897 uid 0
      | [deaddrop:consumer] DEBUG received event seq=…197 app="Contoso Mail" (217 bytes) from producer pid 18897 uid 0
      | [deaddrop:consumer] DEBUG delivering seq=…197 to 1 listener(s)
[listener] recv #1 seq=…197 app='Contoso Mail' ...
```

No files, temp names or renames: the producer queues the event, its delivery thread sends it over the connection, and the consumer checks who connected before accepting anything. On macOS the listener's lines come from the launchd agent's log, streamed into the demo, and the script first prints `(listener started by launchd as gui/501/...)`.

Real lines also carry a timestamp and process id, for example `[deaddrop:consumer] 16:12:20.799 DEBUG pid=2341 ...`. The consumer often reads the file before the producer logs "rename done": the rename is instant, and the producer is still flushing the directory entry to disk.

To control it:

| You want | Run |
|---|---|
| Programs' output only | `DEADDROP_LOG=off samples/run_demo.sh` |
| Every OS notification, including data writes and closes | `DEADDROP_LOG=trace samples/run_demo.sh` |
| No colours (automatic when output is piped) | `NO_COLOR=1 samples/run_demo.sh` |
| Internals while running the programs by hand | add `--debug` to either program, or `export DEADDROP_LOG=debug` |
| Only the library lines from a manual run | `samples/cpp_listener/build/listener --debug 2>&1 \| grep '\[deaddrop:'` |

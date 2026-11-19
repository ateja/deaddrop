# Samples: Python publisher → C++ listener

Two small programs that talk only through deaddrop:

| Program | Role | Prints |
|---|---|---|
| `python_publisher/publisher.py` | Publishes simulated events from a set of fictional apps (Contoso Mail, Fabrikam Notes, ...) | `[publisher] sent seq=... app=... metadata=...` for each event |
| `cpp_listener/listener.cpp` | Listens, prints and collects every event; prints a per app summary on exit | `[listener] recv #n seq=... app=... metadata=...`, then a summary |

Neither program knows about the other. They only agree on the directory.

## One command

From the repository root, inside a Python virtualenv:

```bash
pip install "maturin>=1.7,<2"
```

```bash
samples/run_demo.sh 5
```

This builds the C library, the C++ listener and the Python bindings, then runs both programs. On Windows run it from Git Bash.

Expected output (abridged):

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

## Step by step (two terminals)

Build once:

```bash
cargo build --release -p deaddrop-ffi
```

```bash
cmake -S samples/cpp_listener -B samples/cpp_listener/build && cmake --build samples/cpp_listener/build --config Release
```

```bash
cd crates/deaddrop-py && maturin develop --release && cd ../..
```

Terminal A, listener (runs until Ctrl+C):

```bash
samples/cpp_listener/build/listener
```

On Windows the binary is `samples\cpp_listener\build\Release\listener.exe`.

Terminal B, publisher:

```bash
python samples/python_publisher/publisher.py --count 10 --interval 0.5
```

Both default to `<temp>/deaddrop-demo/events`, so no flags are needed. Start them in either order: the listener waits for the directory, and by default it also delivers events already on disk.

## Options

`publisher.py`

| Flag | Default | Meaning |
|---|---|---|
| `--dir PATH` | `<temp>/deaddrop-demo/events` | shared directory |
| `--count N` | 10 | events to send; `0` = until Ctrl+C |
| `--interval S` | 0.5 | seconds between events |
| `--start-seq N` | current Unix time in ms | first `seq_id` (monotonic across runs) |
| `--seed N` | random | repeatable app choices |

`listener`

| Flag | Default | Meaning |
|---|---|---|
| `--dir PATH` | `<temp>/deaddrop-demo/events` | shared directory |
| `--count N` | unlimited | exit after N events |
| `--timeout S` | none | exit after S seconds |
| `--trust MODE` | `current-user` | who must own the directory: `privileged`, `current-user`, `any` |
| `--new-only` | off | ignore events already on disk |

Listener exit codes: `0` success, `1` error (for example an insecure directory), `2` `--count` not reached before `--timeout`.

`--trust current-user` suits a demo, where you create the folder under your own account. A production consumer should keep the library default, `privileged`, so only a root/SYSTEM-owned directory is trusted.

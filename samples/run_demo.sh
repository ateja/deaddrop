#!/usr/bin/env bash
# Builds the C++ listener and the Python bindings, then runs the Python
# publisher and the C++ listener side by side.
#
#   samples/run_demo.sh [COUNT]
#
# Needs: cargo, cmake, a C++17 compiler, and python with maturin available
# (run inside a virtualenv). Works on macOS, Linux and Windows (Git Bash).
set -euo pipefail

COUNT="${1:-5}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="$ROOT/samples/cpp_listener/build"
PYTHON="${PYTHON:-python}"
command -v "$PYTHON" >/dev/null || PYTHON=python3

echo "==> building the C library (cargo)"
cargo build --release -p deaddrop-ffi --manifest-path "$ROOT/Cargo.toml"

echo "==> building the C++ listener (cmake)"
cmake -S "$ROOT/samples/cpp_listener" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$BUILD" --config Release >/dev/null
LISTENER="$BUILD/listener"
[ -x "$LISTENER" ] || LISTENER="$BUILD/Release/listener.exe"   # MSVC multi-config layout

if ! "$PYTHON" -c "import deaddrop_ipc" 2>/dev/null; then
  echo "==> installing the Python bindings (maturin develop)"
  (cd "$ROOT/crates/deaddrop-py" && maturin develop --release)
fi

# A fresh directory per run. Python builds the native path (C:\... on
# Windows), and bash never has to split it, since Git Bash's dirname does not
# understand backslashes.
DEMO_ROOT="$("$PYTHON" -c 'import os, tempfile; print(os.path.join(tempfile.gettempdir(), "deaddrop-demo-%d" % os.getpid()))')"
case "$DEMO_ROOT" in
  *deaddrop-demo-*) ;;
  *) echo "refusing to use unexpected demo dir: $DEMO_ROOT" >&2; exit 1 ;;
esac
DIR="$DEMO_ROOT/events"
cleanup() { rm -rf "$DEMO_ROOT"; }
trap cleanup EXIT

echo "==> running: listener waits for $COUNT events, publisher sends $COUNT"
echo
# Start the listener first; it waits until the publisher creates the directory.
"$LISTENER" --dir "$DIR" --count "$COUNT" --timeout 30 &
LISTENER_PID=$!
sleep 0.5
"$PYTHON" "$ROOT/samples/python_publisher/publisher.py" --dir "$DIR" --count "$COUNT" --interval 0.2

set +e
wait "$LISTENER_PID"
STATUS=$?
set -e
echo
if [ "$STATUS" -eq 0 ]; then
  echo "==> demo OK: listener received all $COUNT events"
else
  echo "==> demo FAILED: listener exited with $STATUS" >&2
fi
exit "$STATUS"

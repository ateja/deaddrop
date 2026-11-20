#!/usr/bin/env bash
# Builds the C++ listener and the Python bindings, then runs the Python
# publisher and the C++ listener side by side.
#
#   samples/run_demo.sh [COUNT]
#
# Both programs run with DEADDROP_LOG=debug, so the library reports what it
# does behind the scenes: the directory and temp file the producer writes,
# the atomic rename, the OS notification the consumer gets, the file it reads
# and the event it delivers. Those lines are shown indented and coloured
# (yellow = inside the Python publisher, cyan = inside the C++ listener).
#   DEADDROP_LOG=off samples/run_demo.sh   # programs' own output only
#   NO_COLOR=1 samples/run_demo.sh         # no colours (also automatic when piped)
#
# Needs: cargo, cmake, a C++17 compiler, and python (run inside a virtualenv).
# Works on macOS, Linux and Windows (Git Bash).
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

# (Re)install the Python bindings if missing or older than this checkout.
if ! "$PYTHON" -c "import deaddrop_ipc; deaddrop_ipc.set_log_level" 2>/dev/null; then
  echo "==> installing the Python bindings into the active environment (pip install ./crates/deaddrop-py)"
  "$PYTHON" -m pip install --quiet "$ROOT/crates/deaddrop-py"
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

# ---- Behind the scenes ------------------------------------------------------
# The library writes its steps to stderr, one line each, starting with
# "[deaddrop:producer]" or "[deaddrop:consumer]". Each program's stdout and
# stderr share one pipe, so lines keep the order the program printed them in;
# highlight() picks out the library's lines and sets them apart.
export DEADDROP_LOG="${DEADDROP_LOG:-debug}"

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  YELLOW=$'\033[33m' CYAN=$'\033[36m' BOLD=$'\033[1m' RESET=$'\033[0m'
else
  YELLOW='' CYAN='' BOLD='' RESET=''
fi

highlight() {  # $1 = colour for library lines
  local colour=$1 line
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      *"[deaddrop:"*) printf '%s      | %s%s\n' "$colour" "$line" "$RESET" ;;
      *)              printf '%s%s%s\n' "$BOLD" "$line" "$RESET" ;;
    esac
  done
}

echo "==> running: listener waits for $COUNT events, publisher sends $COUNT (DEADDROP_LOG=$DEADDROP_LOG)"
echo
echo "    legend:"
printf '      %s[publisher] ... / [listener] ...%s   what each program prints itself\n' "$BOLD" "$RESET"
case "$DEADDROP_LOG" in
  off|OFF|none|0) echo "      (library internals hidden; unset DEADDROP_LOG to see them)" ;;
  *)
    printf '      %s| [deaddrop:producer] ...%s          library internals inside the Python publisher\n' "$YELLOW" "$RESET"
    printf '      %s| [deaddrop:consumer] ...%s          library internals inside the C++ listener\n' "$CYAN" "$RESET"
    ;;
esac
echo

RC_FILE="$(mktemp)"
# Start the listener first; it waits until the publisher creates the directory.
{
  rc=0
  "$LISTENER" --dir "$DIR" --count "$COUNT" --timeout 30 || rc=$?
  echo "$rc" > "$RC_FILE"
} 2>&1 | highlight "$CYAN" &
VIEW_PID=$!
sleep 0.5
"$PYTHON" -u "$ROOT/samples/python_publisher/publisher.py" --dir "$DIR" --count "$COUNT" --interval 0.2 2>&1 \
  | highlight "$YELLOW"

wait "$VIEW_PID" || true
STATUS="$(cat "$RC_FILE" 2>/dev/null || echo 1)"
rm -f "$RC_FILE"
echo
if [ "$STATUS" -eq 0 ]; then
  echo "==> demo OK: listener received all $COUNT events"
else
  echo "==> demo FAILED: listener exited with $STATUS" >&2
fi
exit "$STATUS"

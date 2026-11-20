#!/usr/bin/env bash
# Builds the C++ listener and the Python bindings, then runs the Python
# publisher and the C++ listener side by side.
#
#   samples/run_demo.sh [COUNT] [file|ipc]
#
# The transport comes from samples/deaddrop.yaml (or the file named by
# DEADDROP_CONFIG); the optional second argument overrides it for one run.
#   file  events are files in a shared folder, picked up by the OS file watcher
#   ipc   events go straight over OS IPC: named pipe (Windows), XPC (macOS),
#         Unix socket (Linux)
# On macOS with ipc, the listener runs as a temporary launchd agent: macOS
# only lets launchd-registered processes listen on an XPC service.
#
# Both programs run with DEADDROP_LOG=debug, so the library reports what it
# does behind the scenes. Those lines are shown indented and coloured
# (yellow = inside the Python publisher, cyan = inside the C++ listener).
#   DEADDROP_LOG=off samples/run_demo.sh   # programs' own output only
#   NO_COLOR=1 samples/run_demo.sh         # no colours (also automatic when piped)
#
# Needs: cargo, cmake, a C++17 compiler, and python (run inside a virtualenv).
# Works on macOS, Linux and Windows (Git Bash).
set -euo pipefail

COUNT="${1:-5}"
TRANSPORT_OVERRIDE="${2:-}"
case "$TRANSPORT_OVERRIDE" in
  ""|file|ipc) ;;
  *) echo "usage: $0 [COUNT] [file|ipc]" >&2; exit 2 ;;
esac
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="$ROOT/samples/cpp_listener/build"
PYTHON="${PYTHON:-python}"
command -v "$PYTHON" >/dev/null || PYTHON=python3
SOURCE_CONFIG="${DEADDROP_CONFIG:-$ROOT/samples/deaddrop.yaml}"

echo "==> building the C library (cargo)"
cargo build --release -p deaddrop-ffi --manifest-path "$ROOT/Cargo.toml"

echo "==> building the C++ listener (cmake)"
cmake -S "$ROOT/samples/cpp_listener" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$BUILD" --config Release >/dev/null
LISTENER="$BUILD/listener"
[ -x "$LISTENER" ] || LISTENER="$BUILD/Release/listener.exe"   # MSVC multi-config layout

# (Re)install the Python bindings if missing or older than this checkout.
if ! "$PYTHON" -c "import deaddrop_ipc as d; d.set_log_level; d.EventProducer.endpoint" 2>/dev/null; then
  echo "==> installing the Python bindings into the active environment (pip install ./crates/deaddrop-py)"
  "$PYTHON" -m pip install --quiet "$ROOT/crates/deaddrop-py"
fi

# A fresh folder per run, holding this run's copy of the config (and, for
# the file transport, the events). Python builds the native path (C:\... on
# Windows), so bash never has to split a path it does not understand.
DEMO_ROOT="$("$PYTHON" -c 'import os, tempfile; print(os.path.join(tempfile.gettempdir(), "deaddrop-demo-%d" % os.getpid()))')"
case "$DEMO_ROOT" in
  *deaddrop-demo-*) ;;
  *) echo "refusing to use unexpected demo dir: $DEMO_ROOT" >&2; exit 1 ;;
esac
DIR="$DEMO_ROOT/events"
CONFIG="$DEMO_ROOT/deaddrop.yaml"

# Copy the config, apply the override, and read back transport + ipc.name.
read -r TRANSPORT IPC_NAME < <("$PYTHON" - "$SOURCE_CONFIG" "$CONFIG" "$TRANSPORT_OVERRIDE" <<'PY'
import os, re, sys
src, dst, override = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(src, encoding="utf-8").read()
if override:
    text, n = re.subn(r"(?m)^transport:.*$", "transport: " + override, text)
    if n == 0:
        text = "transport: %s\n" % override + text
os.makedirs(os.path.dirname(dst), exist_ok=True)
open(dst, "w", encoding="utf-8").write(text)
m = re.search(r"(?m)^transport:\s*([A-Za-z]+)", text)
section = re.search(r"(?ms)^ipc:[ \t]*\n(.*?)(?=^\S|\Z)", text)
name = re.search(r"(?m)^[ \t]+name:[ \t]*[\"']?([A-Za-z0-9._-]+)", section.group(1)) if section else None
print(m.group(1) if m else "file", name.group(1) if name else "com.deaddrop.events")
PY
)
export DEADDROP_CONFIG="$CONFIG"

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

# ---- macOS + ipc: the listener runs under launchd ---------------------------
LAUNCHD=""
if [ "$TRANSPORT" = "ipc" ] && [ "$(uname -s)" = "Darwin" ]; then
  LAUNCHD=1
  LABEL_PREFIX="com.deaddrop.demo.listener"
  LABEL="$LABEL_PREFIX.$$"
  LISTENER_LOG="$DEMO_ROOT/listener.log"
  PLIST="$DEMO_ROOT/$LABEL.plist"
fi

TAIL_PID=""
DOMAIN=""
cleanup() {
  if [ -n "$TAIL_PID" ]; then kill "$TAIL_PID" 2>/dev/null || true; fi
  if [ -n "$DOMAIN" ]; then launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true; fi
  rm -rf "$DEMO_ROOT"
}
trap cleanup EXIT

start_listener_under_launchd() {
  # Remove agents a previous, interrupted run may have left behind: two jobs
  # cannot both own the same Mach service name.
  for old in $(launchctl list | awk -v p="$LABEL_PREFIX." 'index($3, p) == 1 { print $3 }'); do
    launchctl bootout "gui/$(id -u)/$old" 2>/dev/null || launchctl bootout "user/$(id -u)/$old" 2>/dev/null || true
  done
  : > "$LISTENER_LOG"
  cat > "$PLIST" <<PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$LISTENER</string>
    <string>--dir</string><string>$DIR</string>
    <string>--count</string><string>$COUNT</string>
    <string>--timeout</string><string>30</string>
  </array>
  <!-- Registers the XPC service the listener checks in to. -->
  <key>MachServices</key>
  <dict><key>$IPC_NAME</key><true/></dict>
  <key>EnvironmentVariables</key>
  <dict>
    <key>DEADDROP_CONFIG</key><string>$CONFIG</string>
    <key>DEADDROP_LOG</key><string>$DEADDROP_LOG</string>
  </dict>
  <key>StandardOutPath</key><string>$LISTENER_LOG</string>
  <key>StandardErrorPath</key><string>$LISTENER_LOG</string>
  <key>RunAtLoad</key><true/>
</dict>
</plist>
PLIST_EOF
  DOMAIN="gui/$(id -u)"
  if ! launchctl bootstrap "$DOMAIN" "$PLIST" 2>/dev/null; then
    DOMAIN="user/$(id -u)"
    launchctl bootstrap "$DOMAIN" "$PLIST"
  fi
  echo "    (listener started by launchd as $DOMAIN/$LABEL, serving XPC service $IPC_NAME)"
  echo
  # Stream its output into the demo, coloured like a directly started listener.
  { tail -n +1 -f "$LISTENER_LOG" & echo $! > "$DEMO_ROOT/tail.pid"; wait; } 2>/dev/null | highlight "$CYAN" &
  sleep 0.2
  TAIL_PID="$(cat "$DEMO_ROOT/tail.pid")"
}

wait_for_launchd_listener() {  # prints the listener's exit code
  local i state code
  for i in $(seq 1 200); do
    state="$(launchctl print "$DOMAIN/$LABEL" 2>/dev/null | awk -F'= ' '/^\tstate = / { print $2; exit }')"
    if [ "$state" != "running" ] && grep -q "\[listener\] collected" "$LISTENER_LOG" 2>/dev/null; then
      break
    fi
    sleep 0.2
  done
  sleep 0.3  # let tail flush the last lines
  code="$(launchctl print "$DOMAIN/$LABEL" 2>/dev/null | awk -F'= ' '/last exit code/ { print $2; exit }' | tr -dc '0-9')"
  echo "${code:-1}"
}

# ---- run --------------------------------------------------------------------
case "$TRANSPORT" in
  ipc)
    case "$(uname -s)" in
      Darwin) WHERE="XPC service $IPC_NAME" ;;
      MINGW*|MSYS*|CYGWIN*) WHERE="named pipe \\\\.\\pipe\\$IPC_NAME" ;;
      *) WHERE="Unix socket /tmp/$IPC_NAME.sock" ;;
    esac ;;
  *) WHERE="shared folder $DIR" ;;
esac
# printf, not echo: some bash builds treat "\c" (as in \\.\pipe\com...) as "stop output".
printf '==> transport: %s (%s), from %s%s\n' "$TRANSPORT" "$WHERE" "$SOURCE_CONFIG" "${TRANSPORT_OVERRIDE:+ (overridden on the command line)}"
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

if [ -n "$LAUNCHD" ]; then
  start_listener_under_launchd
  "$PYTHON" -u "$ROOT/samples/python_publisher/publisher.py" --dir "$DIR" --count "$COUNT" --interval 0.2 2>&1 \
    | highlight "$YELLOW"
  STATUS="$(wait_for_launchd_listener)"
else
  RC_FILE="$(mktemp)"
  # Start the listener first. With file it waits for the publisher to create
  # the folder; with ipc it opens the endpoint the publisher connects to.
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
fi

echo
if [ "$STATUS" -eq 0 ]; then
  echo "==> demo OK ($TRANSPORT): listener received all $COUNT events"
else
  echo "==> demo FAILED ($TRANSPORT): listener exited with $STATUS" >&2
fi
exit "$STATUS"

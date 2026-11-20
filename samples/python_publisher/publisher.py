#!/usr/bin/env python3
"""Sample producer: publishes simulated detection events.

Every published event is printed to stdout. Pair it with the C++ listener in
samples/cpp_listener, pointed at the same directory.

    pip install ./crates/deaddrop-py  # from the repo root, inside a venv
    python publisher.py --count 10 --interval 0.5
"""

import argparse
import itertools
import json
import os
import platform
import random
import sys
import tempfile
import time

from deaddrop_ipc import DetectionEvent, EventProducer, IpcError, set_log_level

DEFAULT_DIR = os.path.join(tempfile.gettempdir(), "deaddrop-demo", "events")

# Fictional applications: name, vendor, how it was observed.
SAMPLE_APPS = [
    ("Contoso Mail", "Contoso", "process"),
    ("Fabrikam Notes", "Fabrikam", "process"),
    ("Northwind Sync", "Northwind", "service"),
    ("Litware Viewer", "Litware", "plugin"),
    ("Tailspin Player", "Tailspin", "process"),
    ("Adatum Reports", "Adatum", "service"),
]


def parse_args():
    p = argparse.ArgumentParser(description="Publish sample detection events.")
    p.add_argument("--dir", default=DEFAULT_DIR, help=f"shared directory (default: {DEFAULT_DIR})")
    p.add_argument("--count", type=int, default=10, help="events to publish; 0 = until Ctrl+C (default: 10)")
    p.add_argument("--interval", type=float, default=0.5, help="seconds between events (default: 0.5)")
    p.add_argument(
        "--start-seq",
        type=int,
        default=None,
        help="first seq_id (default: current Unix time in ms, so runs never collide)",
    )
    p.add_argument("--seed", type=int, default=None, help="random seed for repeatable output")
    p.add_argument(
        "--debug",
        action="store_true",
        help="print the library's internal steps to stderr (same as DEADDROP_LOG=debug)",
    )
    return p.parse_args()


def make_event(seq_id, rng):
    app, vendor, method = rng.choice(SAMPLE_APPS)
    return DetectionEvent(
        seq_id=seq_id,
        app_name=app,
        metadata={
            "vendor": vendor,
            "detection_method": method,
            "confidence": rng.randint(70, 100),
            "pid": rng.randint(1000, 65000),
            "host": platform.node() or "unknown",
        },
    )


def main():
    args = parse_args()
    if args.debug:
        set_log_level("debug")
    rng = random.Random(args.seed)
    start = args.start_seq if args.start_seq is not None else time.time_ns() // 1_000_000

    try:
        producer = EventProducer.init(args.dir)
    except IpcError as e:
        print(f"[publisher] cannot open {args.dir}: {e}", file=sys.stderr)
        return 1

    print(f"[publisher] writing to {args.dir}", flush=True)
    seqs = itertools.count(start) if args.count == 0 else range(start, start + args.count)
    published = 0
    try:
        with producer:
            for n, seq_id in enumerate(seqs):
                if n and args.interval > 0:
                    time.sleep(args.interval)
                event = make_event(seq_id, rng)
                producer.publish_event(event)
                published += 1
                print(
                    f"[publisher] sent seq={event.seq_id} app={event.app_name!r} "
                    f"at={event.timestamp_utc} metadata={json.dumps(event.metadata, sort_keys=True)}",
                    flush=True,
                )
    except KeyboardInterrupt:
        pass
    except IpcError as e:
        print(f"[publisher] publish failed: {e}", file=sys.stderr)
        return 1

    print(f"[publisher] done, published {published} event(s)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

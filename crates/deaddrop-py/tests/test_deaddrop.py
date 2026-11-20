import json
import os
import queue
import stat
import sys
import threading
import time

import pytest

from deaddrop_ipc import (
    SCHEMA_VERSION,
    DetectionEvent,
    EventConsumer,
    EventProducer,
    InsecureDirectoryError,
    InvalidEventError,
    ShutDownError,
)

WAIT = 5.0


def make_consumer(path, **kw):
    q = queue.Queue()
    kw.setdefault("trusted_owner", "current_user")
    consumer = EventConsumer.init(str(path), **kw)
    consumer.register_event_listener(q.put)
    consumer.start_watching()
    return consumer, q


def test_proposal_snippet_verbatim(tmp_path):
    """The producer code from the design doc, unchanged except the path."""
    events = tmp_path / "events"
    producer = EventProducer.init(str(events))
    event = DetectionEvent(
        schema_version="1.0",
        seq_id=10492,
        timestamp_utc="2026-11-18T17:45:00Z",
        app_name="TargetApp",
        metadata={"vendor": "ExampleCorp", "confidence": 98},
    )
    producer.publish_event(event)
    producer.shutdown()

    files = [f for f in os.listdir(events) if f.startswith("evt-")]
    assert len(files) == 1
    on_disk = json.loads((events / files[0]).read_text())
    assert on_disk == {
        "schema_version": "1.0",
        "seq_id": 10492,
        "timestamp_utc": "2026-11-18T17:45:00Z",
        "app_name": "TargetApp",
        "metadata": {"vendor": "ExampleCorp", "confidence": 98},
    }


def test_roundtrip_through_watcher(tmp_path):
    events = tmp_path / "events"
    with EventProducer.init(events) as producer:
        consumer, q = make_consumer(events, rescan_interval_seconds=None)
        for i in range(50):
            producer.publish_event(DetectionEvent(i, "TargetApp", metadata={"i": i, "nested": {"ok": [1, 2]}}))
        got = [q.get(timeout=WAIT) for _ in range(50)]
        consumer.shutdown()
    assert [e.seq_id for e in got] == list(range(50))
    assert got[7].metadata == {"i": 7, "nested": {"ok": [1, 2]}}
    assert got[0].schema_version == SCHEMA_VERSION
    assert got[0].timestamp_utc.endswith("Z")


def test_event_value_semantics():
    a = DetectionEvent(1, "A", timestamp_utc="2026-11-18T17:45:00Z", metadata={"k": 1})
    b = DetectionEvent.from_json(a.to_json())
    assert a == b
    m = a.metadata
    m["k"] = 2  # returned dict is a copy
    assert a.metadata == {"k": 1}
    assert "seq_id=1" in repr(a)


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(seq_id=1, app_name=""),
        dict(seq_id=1, app_name="A", schema_version="2.0"),
        dict(seq_id=1, app_name="A", timestamp_utc="yesterday"),
    ],
)
def test_invalid_events_rejected(kwargs):
    with pytest.raises(InvalidEventError):
        DetectionEvent(**kwargs)


def test_bad_metadata():
    with pytest.raises(TypeError):
        DetectionEvent(1, "A", metadata=[1, 2])
    with pytest.raises(ValueError):
        DetectionEvent(1, "A", metadata={"x": float("nan")})
    with pytest.raises(TypeError):
        DetectionEvent(1, "A", metadata={"x": object()})
    with pytest.raises(OverflowError):
        DetectionEvent(-1, "A")


def test_too_large_and_shutdown(tmp_path):
    p = EventProducer.init(tmp_path / "e", max_event_bytes=256)
    with pytest.raises(InvalidEventError):
        p.publish_event(DetectionEvent(1, "A", metadata={"blob": "x" * 1000}))
    p.shutdown()
    assert p.closed
    with pytest.raises(ShutDownError):
        p.publish_event(DetectionEvent(2, "A"))


def test_callback_exception_does_not_stop_delivery(tmp_path, capsys):
    events = tmp_path / "events"
    producer = EventProducer.init(events)
    q = queue.Queue()

    def flaky(e):
        if e.seq_id == 1:
            raise RuntimeError("boom")
        q.put(e.seq_id)

    with EventConsumer.init(events, trusted_owner="current_user") as consumer:
        consumer.register_event_listener(flaky)
        consumer.start_watching()
        producer.publish_event(DetectionEvent(1, "A"))
        producer.publish_event(DetectionEvent(2, "A"))
        assert q.get(timeout=WAIT) == 2
    assert "boom" in capsys.readouterr().err


def test_shutdown_while_callback_needs_gil(tmp_path):
    """Shutdown must release the GIL, or a callback blocked on it deadlocks."""
    events = tmp_path / "events"
    producer = EventProducer.init(events)
    started = threading.Event()

    def slow(_e):
        started.set()
        time.sleep(0.3)

    consumer = EventConsumer.init(events, trusted_owner="current_user")
    consumer.register_event_listener(slow)
    consumer.start_watching()
    for i in range(5):
        producer.publish_event(DetectionEvent(i, "A"))
    assert started.wait(WAIT)
    t = time.monotonic()
    consumer.shutdown()
    assert time.monotonic() - t < 3.0
    with pytest.raises(ShutDownError):
        consumer.start_watching()


def test_gc_of_running_consumer_does_not_hang(tmp_path):
    events = tmp_path / "events"
    producer = EventProducer.init(events)
    consumer, q = make_consumer(events)
    producer.publish_event(DetectionEvent(1, "A"))
    q.get(timeout=WAIT)
    del consumer  # Drop joins the worker with the GIL released


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX modes")
def test_rejects_world_writable_dir(tmp_path):
    d = tmp_path / "events"
    d.mkdir()
    d.chmod(0o777)
    with pytest.raises(InsecureDirectoryError):
        EventProducer.init(d)
    c = EventConsumer.init(d, trusted_owner="current_user")
    with pytest.raises(InsecureDirectoryError):
        c.start_watching()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX modes")
def test_custom_modes(tmp_path):
    d = tmp_path / "events"
    p = EventProducer.init(d, dir_mode=0o750, file_mode=0o640)
    p.publish_event(DetectionEvent(1, "A"))
    assert stat.S_IMODE(d.stat().st_mode) == 0o750
    f = next(d.glob("evt-*.json"))
    assert stat.S_IMODE(f.stat().st_mode) == 0o640
    with pytest.raises(ValueError):
        EventProducer.init(tmp_path / "x", file_mode=0o666)


def test_trusted_owner_argument_validation(tmp_path):
    with pytest.raises(ValueError):
        EventConsumer.init(tmp_path, trusted_owner="root-ish")


def test_set_log_level(tmp_path, capfd):
    from deaddrop_ipc import set_log_level

    with pytest.raises(ValueError):
        set_log_level("loud")
    set_log_level("debug")
    try:
        p = EventProducer.init(tmp_path / "events")
        p.publish_event(DetectionEvent(1, "A"))
        p.shutdown()
    finally:
        set_log_level("warn")
    err = capfd.readouterr().err
    assert "[deaddrop:producer]" in err
    assert "atomic rename .tmp-" in err
    assert "now visible to consumers" in err

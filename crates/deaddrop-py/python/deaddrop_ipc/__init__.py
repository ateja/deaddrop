"""Local, decoupled event exchange between endpoint applications.

    from deaddrop_ipc import EventProducer, DetectionEvent

    producer = EventProducer.init("/var/run/vendor_ipc/events")
    producer.publish_event(DetectionEvent(seq_id=1, app_name="TargetApp"))
    producer.shutdown()
"""

from ._native import (
    SCHEMA_VERSION,
    DetectionEvent,
    EventConsumer,
    EventProducer,
    InsecureDirectoryError,
    InvalidEventError,
    IpcError,
    ShutDownError,
    __version__,
    set_log_level,
)

__all__ = [
    "SCHEMA_VERSION",
    "DetectionEvent",
    "EventConsumer",
    "EventProducer",
    "InsecureDirectoryError",
    "InvalidEventError",
    "IpcError",
    "ShutDownError",
    "__version__",
    "set_log_level",
]

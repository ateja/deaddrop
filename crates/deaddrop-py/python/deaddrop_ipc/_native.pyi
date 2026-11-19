from os import PathLike
from types import TracebackType
from typing import Any, Callable, Dict, Literal, Optional, Type, Union

SCHEMA_VERSION: str
__version__: str

class IpcError(Exception): ...
class InsecureDirectoryError(IpcError): ...
class InvalidEventError(IpcError): ...
class ShutDownError(IpcError): ...

class DetectionEvent:
    def __init__(
        self,
        seq_id: int,
        app_name: str,
        schema_version: Optional[str] = None,
        timestamp_utc: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None: ...
    @property
    def schema_version(self) -> str: ...
    @property
    def seq_id(self) -> int: ...
    @property
    def timestamp_utc(self) -> str: ...
    @property
    def app_name(self) -> str: ...
    @property
    def metadata(self) -> Dict[str, Any]: ...
    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "DetectionEvent": ...

class EventProducer:
    @staticmethod
    def init(
        path: Union[str, "PathLike[str]"],
        *,
        durable: bool = True,
        max_files: Optional[int] = 1024,
        max_age_seconds: Optional[float] = 86400.0,
        max_event_bytes: int = 1048576,
        dir_mode: Optional[int] = None,
        file_mode: Optional[int] = None,
        group: Optional[int] = None,
    ) -> "EventProducer": ...
    def publish_event(self, event: DetectionEvent) -> None: ...
    def shutdown(self) -> None: ...
    @property
    def closed(self) -> bool: ...
    def __enter__(self) -> "EventProducer": ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool: ...

class EventConsumer:
    @staticmethod
    def init(
        path: Union[str, "PathLike[str]"],
        *,
        trusted_owner: Union[Literal["privileged", "current_user", "any"], int, None] = None,
        replay_existing: bool = True,
        rescan_interval_seconds: Optional[float] = 5.0,
        max_event_bytes: int = 1048576,
    ) -> "EventConsumer": ...
    def register_event_listener(self, callback: Callable[[DetectionEvent], Any]) -> None: ...
    def start_watching(self) -> None: ...
    def shutdown(self) -> None: ...
    def __enter__(self) -> "EventConsumer": ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool: ...

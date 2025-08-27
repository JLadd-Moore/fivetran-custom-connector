from __future__ import annotations

from typing import Any, Mapping

from fivetran_connector_sdk import Logging as _sdk_log


class EventLogger:
    """Structured single-line event logger that wraps the SDK logger.

    Format: "[connector] [service] event_type (k=v, k=v)"
    """

    def __init__(self, connector: str, service: str) -> None:
        self._connector = connector
        self._service = service

    def _format(self, event_type: str, details: Mapping[str, Any] | None) -> str:
        prefix = f"[{self._connector}] [{self._service}] {event_type}"
        if details:
            parts = []
            for key, val in details.items():
                try:
                    rendered = str(val)
                except Exception:
                    rendered = "<unrepr>"
                parts.append(f"{key}={rendered}")
            return f"{prefix} (" + ", ".join(parts) + ")"
        return prefix

    def debug(self, event_type: str, **details: Any) -> None:
        _sdk_log.debug(self._format(event_type, details))

    def info(self, event_type: str, **details: Any) -> None:
        _sdk_log.info(self._format(event_type, details))

    def warning(self, event_type: str, **details: Any) -> None:
        _sdk_log.warning(self._format(event_type, details))

    def error(self, event_type: str, **details: Any) -> None:
        _sdk_log.error(self._format(event_type, details))


def get_logger(connector: str, service: str) -> EventLogger:
    return EventLogger(connector=connector, service=service)


__all__ = ["EventLogger", "get_logger"]



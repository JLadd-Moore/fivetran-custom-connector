from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, Mapping, Optional, Sequence, Union

import requests
from marshmallow import Schema


HttpMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]

GetNextPageFn = Callable[
    [requests.Response, Mapping[str, Any]], Optional[Mapping[str, Any]]
]
ParseDownloadUrlFn = Callable[[requests.Response], Optional[str]]
ExtractItemsFn = Callable[[Any], Iterable[Any]]


@dataclass(slots=True)
class Endpoint:
    """Declarative description of an API endpoint and how to paginate/parse it.

    Attributes
    - name: logical name used to reference this endpoint from the client
    - path: relative or absolute path/URL for the endpoint
    - method: HTTP method to use
    - default_params: parameters merged with call-time params
    - request_schema: REQUIRED schema used to dump/validate outbound request JSON bodies
    - response_schema: REQUIRED schema used to load/validate inbound response JSON
    - get_next_page: callable computing parameters for the next page, or None
    - parse_download_url: callable to extract a download URL from the initial response
    - extract_items: callable that takes the validated response payload and returns
      an iterable of unit objects that constitute a page
    """

    name: str
    path: str
    request_schema: Schema
    response_schema: Schema
    default_params: Mapping[str, Any] | None = None
    method: HttpMethod = "GET"
    get_next_page: Optional[GetNextPageFn] = None
    parse_download_url: Optional[ParseDownloadUrlFn] = None
    extract_items: Optional[ExtractItemsFn] = None

    def build_url(self, base_url: Optional[str]) -> str:
        if self.path.startswith("http://") or self.path.startswith("https://"):
            return self.path
        if not base_url:
            return self.path
        if base_url.endswith("/") and self.path.startswith("/"):
            return base_url[:-1] + self.path
        if not base_url.endswith("/") and not self.path.startswith("/"):
            return base_url + "/" + self.path
        return base_url + self.path


def items_path_extractor(path: Union[str, Sequence[Union[str, int]]]) -> ExtractItemsFn:
    """Create an extractor that traverses a dot-separated path to a list.

    Example: items_path_extractor("properties.periods")
    """
    if isinstance(path, str):
        parts: Sequence[Union[str, int]] = [p for p in path.split(".") if p]
    else:
        parts = path

    def _extract(payload: Any) -> Iterable[Any]:
        current: Any = payload
        for part in parts:
            if isinstance(current, Mapping) and isinstance(part, str):
                current = current.get(part)
            elif (
                isinstance(current, Sequence)
                and not isinstance(current, (str, bytes))
                and isinstance(part, int)
            ):
                current = current[part]
            else:
                return []
            if current is None:
                return []
        if isinstance(current, list):
            return current
        return [current]

    return _extract


__all__ = [
    "Endpoint",
    "HttpMethod",
    "GetNextPageFn",
    "ParseDownloadUrlFn",
    "ExtractItemsFn",
    "items_path_extractor",
]

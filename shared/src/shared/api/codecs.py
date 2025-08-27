from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Dict, List

import requests

try:
    # Prefer lxml for robust XPath and namespace handling
    from lxml import etree as _etree  # type: ignore

    _HAS_LXML = True
except Exception:  # pragma: no cover - fallback
    import xml.etree.ElementTree as _etree  # type: ignore

    _HAS_LXML = False


@dataclass
class RequestParts:
    params: Optional[Mapping[str, Any]] = None
    json: Optional[Mapping[str, Any]] = None
    data: Optional[bytes] = None
    headers: Optional[Mapping[str, str]] = None


class PayloadCodec:
    """Strategy that translates request fields to HTTP request parts and parses responses."""

    def dump(
        self, endpoint: Any, request_fields: Mapping[str, Any]
    ) -> RequestParts:  # pragma: no cover - interface
        raise NotImplementedError

    def load(self, response: requests.Response) -> Any:  # pragma: no cover - interface
        raise NotImplementedError


class JsonCodec(PayloadCodec):
    """Default behavior matching prior JSON semantics in ApiClient."""

    def dump(self, endpoint: Any, request_fields: Mapping[str, Any]) -> RequestParts:
        if getattr(endpoint, "method", "GET") == "GET":
            return RequestParts(params=request_fields or None, headers=None)
        return RequestParts(
            json=request_fields or None, headers={"Content-Type": "application/json"}
        )

    def load(self, response: requests.Response) -> Any:
        response.raise_for_status()
        return response.json()


class SoapCodec(PayloadCodec):
    """SOAP 1.1 codec that builds envelopes and parses XML.

    Parameters
    - action: SOAPAction header value
    - envelope_builder: callable mapping request_fields -> XML string (full Envelope)
    - namespaces: optional default namespaces for parsing fault detection, etc.
    """

    def __init__(
        self,
        *,
        action: str,
        envelope_builder: Callable[[Mapping[str, Any]], str],
        namespaces: Optional[Dict[str, str]] = None,
    ) -> None:
        self._action = action
        self._envelope_builder = envelope_builder
        self._namespaces = namespaces or {}

    def dump(self, endpoint: Any, request_fields: Mapping[str, Any]) -> RequestParts:
        xml_str = self._envelope_builder(request_fields or {})
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": self._action,
        }
        return RequestParts(data=xml_str.encode("utf-8"), headers=headers)

    def load(self, response: requests.Response) -> Any:
        response.raise_for_status()
        content = response.content or b""
        if not content:
            return None
        root = _etree.fromstring(content)

        # Detect SOAP Fault in a namespace-agnostic way
        if _HAS_LXML:
            fault_nodes = root.xpath("//*[local-name() = 'Fault']")  # type: ignore[attr-defined]
            if fault_nodes:
                raise RuntimeError(
                    "SOAP Fault: " + _etree.tostring(fault_nodes[0], encoding="unicode")
                )
        else:
            # xml.etree doesn't support local-name(), do best-effort check
            def _local(tag: str) -> str:
                return tag.split("}", 1)[-1]

            for elem in root.iter():
                if _local(getattr(elem, "tag", "")) == "Fault":
                    raise RuntimeError("SOAP Fault encountered")

        return root


class CsvCodec(PayloadCodec):
    """CSV codec with optional streaming.

    If stream=True, returns an iterator of row dicts under key "results_iter";
    otherwise, returns {"results": [row, ...]}.
    """

    def __init__(self, *, delimiter: Optional[str] = None, stream: bool = True) -> None:
        self._delimiter = delimiter
        self._stream = stream

    def dump(self, endpoint: Any, request_fields: Mapping[str, Any]) -> RequestParts:
        return RequestParts(headers=None)

    def load(self, response: requests.Response) -> Any:
        import csv
        import io

        response.raise_for_status()
        text = response.text or ""
        if not text:
            return {"results": []} if not self._stream else {"results_iter": iter(())}

        # Determine delimiter from first line if not set
        first_newline = text.find("\n")
        first_line = text[: first_newline if first_newline != -1 else len(text)]
        delimiter = self._delimiter or ("\t" if "\t" in first_line else ",")

        buf = io.StringIO(text)
        reader = csv.DictReader(buf, delimiter=delimiter)
        if self._stream:
            return {"results_iter": reader}
        else:
            return {"results": list(reader)}


__all__ = [
    "PayloadCodec",
    "JsonCodec",
    "SoapCodec",
    "CsvCodec",
    "RequestParts",
]

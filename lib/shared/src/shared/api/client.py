from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Sequence

import requests
from fivetran_connector_sdk import (
    Logging as log,
)

from .endpoint import Endpoint
from .auth_strategy import AuthStrategy


def iteritems(endpoint_result):
    for page in endpoint_result:
        for item in page:
            yield item


class ApiClient:
    """HTTP API client that wires together `AuthStrategy` and `Endpoint` configs.

    Usage
        auth = BearerAuth(token="...")
        api = ApiClient(base_url="https://api.example.com", auth_strategy=auth, endpoints=[...])
        for item in api.endpoint("people").items({"page_size": 100}):
            ...
    """

    def __init__(
        self,
        auth_strategy: AuthStrategy,
        endpoints: Sequence[Endpoint],
        session_factory: Callable[[], requests.Session] | None = None,
    ) -> None:
        self._base_url = None
        self._session = (session_factory or requests.Session)()
        self._auth_strategy = auth_strategy
        self._endpoints: Dict[str, Endpoint] = {ep.name: ep for ep in endpoints}

        self._auth_strategy.apply(self._session)

    def endpoint(self, name: str) -> "_EndpointHandle":
        endpoint = self._endpoints.get(name)
        if endpoint is None:
            raise KeyError(f"Endpoint not found: {name}")
        return _EndpointHandle(self._session, self._base_url, endpoint)


class _EndpointHandle:
    """Lightweight invoker for a single endpoint configuration."""

    def __init__(
        self, session: requests.Session, base_url: Optional[str], endpoint: Endpoint
    ) -> None:
        self._session = session
        self._base_url = base_url
        self._endpoint = endpoint

    def pages(
        self, params: Optional[Mapping[str, Any]] = None
    ) -> Iterator[Iterable[Any]]:
        """Execute the endpoint and iterate over pages of results (page-wise).

        Unified parameter semantics:
        - Top-level keys (except reserved ones) are treated as request fields
          and validated/dumped via the endpoint's request_schema.
          - For GET endpoints: sent as query parameters
          - For non-GET endpoints: sent as JSON body
        - Reserved keys:
          - "headers": additional request headers
          - "timeout": optional requests timeout value

        Backwards compatibility:
        - If "query" is provided for GET, it is used as the request fields.
        - If "json" is provided for non-GET, it is used as the request fields.
        - get_next_page may return either a flat mapping of request fields, or
          a mapping with "query" (GET) or "json" (non-GET) to replace fields.
        """
        effective_params: Dict[str, Any] = {}
        if self._endpoint.default_params:
            effective_params.update(dict(self._endpoint.default_params))
        if params:
            effective_params.update(dict(params))

        # Reserved keys not part of request fields
        extra_headers: Mapping[str, str] = effective_params.get("headers", {}) or {}
        timeout: Optional[float] = effective_params.get("timeout")

        # Determine request fields with backwards compatibility
        request_fields: Optional[Mapping[str, Any]]
        if self._endpoint.method == "GET":
            if "query" in effective_params:
                request_fields = effective_params.get("query") or {}
            else:
                request_fields = {
                    k: v
                    for k, v in effective_params.items()
                    if k not in {"headers", "timeout", "json"}
                }
        else:
            if "json" in effective_params:
                request_fields = effective_params.get("json") or {}
            else:
                request_fields = {
                    k: v
                    for k, v in effective_params.items()
                    if k not in {"headers", "timeout"}
                }

        # Validate/dump via schema
        dumped_fields: Optional[Mapping[str, Any]] = None
        if request_fields is not None:
            dumped_fields = self._endpoint.request_schema.dump(request_fields)

        url = self._endpoint.build_url(self._base_url)

        while True:
            # log.info(f"Requesting {url} with params {dumped_fields}")
            response = self._session.request(
                method=self._endpoint.method,
                url=url,
                params=dumped_fields if self._endpoint.method == "GET" else None,
                json=dumped_fields if self._endpoint.method != "GET" else None,
                headers=extra_headers or None,
                timeout=timeout,
            )
            response.raise_for_status()

            download_url: Optional[str] = None
            if self._endpoint.parse_download_url is not None:
                download_url = self._endpoint.parse_download_url(response)

            if download_url:
                download_response = self._session.get(download_url, timeout=timeout)
                download_response.raise_for_status()
                page_payload: Any
                content_type = download_response.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    page_payload = download_response.json()
                else:
                    page_payload = download_response.content
            else:
                page_payload = response.json()

            if isinstance(page_payload, (dict, list)):
                page_data = self._endpoint.response_schema.load(page_payload)
            else:
                page_data = page_payload

            # Extract unit objects for this page
            if self._endpoint.extract_items is not None:
                items_iter = self._endpoint.extract_items(page_data)
            else:
                if isinstance(page_data, list):
                    items_iter = page_data
                elif isinstance(page_data, dict) and "results" in page_data:
                    items_iter = page_data["results"]
                else:
                    items_iter = [page_data]

            # Ensure we yield a concrete list for predictable iteration
            items_list = (
                list(items_iter) if not isinstance(items_iter, list) else items_iter
            )
            yield items_list

            if self._endpoint.get_next_page is None:
                break
            next_params = self._endpoint.get_next_page(response, effective_params)
            if not next_params:
                break

            # Apply pagination params (flat mapping by default). Support explicit
            # {"query": {...}} or {"json": {...}} for backwards compatibility.
            if self._endpoint.method == "GET":
                if "query" in next_params:
                    request_fields = next_params["query"] or {}
                else:
                    request_fields = {**(request_fields or {}), **next_params}
            else:
                if "json" in next_params:
                    request_fields = next_params["json"] or {}
                else:
                    request_fields = {**(request_fields or {}), **next_params}

            # Recompute dumped fields for the next loop
            dumped_fields = (
                self._endpoint.request_schema.dump(request_fields)
                if request_fields is not None
                else None
            )

    def items(self, params: Optional[Mapping[str, Any]] = None) -> Iterator[Any]:
        """Execute the endpoint and iterate over items (de-paginated).

        This flattens pages on-demand so callers can simply iterate items.
        Use .pages(...) if page-wise iteration is needed.
        """
        for page in self.pages(params):
            for item in page:
                yield item

    def get(self, params: Optional[Mapping[str, Any]] = None) -> Iterator[Any]:
        """Backward-compatible alias for .items(...)."""
        return self.items(params)


__all__ = [
    "ApiClient",
]

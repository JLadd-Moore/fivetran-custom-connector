from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Sequence

import requests

from .endpoint import Endpoint
from .auth_strategy import AuthStrategy


class ApiClient:
    """HTTP API client that wires together `AuthStrategy` and `Endpoint` configs.

    Usage
        auth = BearerAuth(token="...")
        api = ApiClient(base_url="https://api.example.com", auth_strategy=auth, endpoints=[...])
        for page in api.endpoint("people").get({"query": {"page_size": 100}}):
            for item in page:
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

    def get(
        self, params: Optional[Mapping[str, Any]] = None
    ) -> Iterator[Iterable[Any]]:
        """Execute the endpoint and iterate over pages of results.

        Parameters semantics (convention-based for now):
        - params["query"]: mapping passed as query parameters
        - params["json"]: mapping passed as JSON body for non-GET methods
        - params["headers"]: additional request headers to merge for this call
        - params["timeout"]: optional requests timeout value
        """
        effective_params: Dict[str, Any] = {}
        if self._endpoint.default_params:
            effective_params.update(dict(self._endpoint.default_params))
        if params:
            effective_params.update(dict(params))

        query_params: Mapping[str, Any] = effective_params.get("query", {}) or {}
        json_body: Optional[Mapping[str, Any]] = effective_params.get("json")
        extra_headers: Mapping[str, str] = effective_params.get("headers", {}) or {}
        timeout: Optional[float] = effective_params.get("timeout")

        if json_body is not None:
            json_body = self._endpoint.request_schema.dump(json_body)  # type: ignore[arg-type]

        url = self._endpoint.build_url(self._base_url)

        while True:
            response = self._session.request(
                method=self._endpoint.method,
                url=url,
                params=query_params if self._endpoint.method == "GET" else None,
                json=json_body if self._endpoint.method != "GET" else None,
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
                page_data = self._endpoint.response_schema.load(page_payload)  # type: ignore[arg-type]
            else:
                page_data = page_payload

            # Extract unit objects for this page
            if self._endpoint.extract_items is not None:
                items_iter = self._endpoint.extract_items(page_data)
            else:
                if isinstance(page_data, list):
                    items_iter = page_data
                elif isinstance(page_data, dict) and "results" in page_data:
                    items_iter = page_data["results"]  # type: ignore[index]
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

            if "query" in next_params:
                query_params = next_params["query"]  # type: ignore[assignment]
            else:
                query_params = {**query_params, **next_params}  # type: ignore[assignment]


__all__ = [
    "ApiClient",
]

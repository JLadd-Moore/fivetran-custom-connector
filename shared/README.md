# Shared API Utils for Fivetran Connectors

Lightweight, composable building blocks for HTTP-based connectors (REST, SOAP, CSV downloads) used by connectors in this repository.

- ApiClient: wires an authentication strategy and declarative Endpoint configs, exposing page-wise and item-wise iteration
- Endpoint: describes HTTP method, path, request/response schemas, pagination, extraction, and optional codecs
- Auth strategies: NoAuth, BasicAuth, BearerAuth, OAuth2RefreshTokenAuth (auto-refresh on 401)
- Codecs: JsonCodec (default), SoapCodec (SOAP 1.1 XML), CsvCodec (inline CSV payloads)
- XML helpers: extractors mapping XPath to dicts or Marshmallow schemas
- Logging: EventLogger for single-line structured logging

## Quickstart (REST with nextPageLink pagination)

```python
from urllib.parse import urlparse, parse_qs
from marshmallow import Schema, INCLUDE
from shared.api.client import ApiClient
from shared.api.endpoint import Endpoint
from shared.api.auth_strategy import BasicAuth

class EmptySchema(Schema):
    class Meta:
        unknown = INCLUDE

class IdentitySchema(Schema):
    class Meta:
        unknown = INCLUDE
    def dump(self, obj, *, many=None):  # type: ignore[override]
        return obj or {}

def _next_params_from_nextpagelink(_resp, payload, _params):
    if not isinstance(payload, dict) or not payload.get("nextPageLink"):
        return None
    parsed = urlparse(payload["nextPageLink"]) 
    qs = parse_qs(parsed.query)
    flat = {k: (v[0] if isinstance(v, list) and v else v) for k, v in qs.items()}
    return {"query": flat}  # replaces the entire query for the next request

api = ApiClient(
    auth_strategy=BasicAuth(
        username=cfg["username"],
        password=cfg["password"],
        extra_headers={"Accept": "application/json"},
    ),
    endpoints=[
        Endpoint(
            name="people",
            path="https://api.example.com/v1/people",
            method="GET",
            request_schema=IdentitySchema(),
            response_schema=EmptySchema(),
            extract_items=lambda payload: (payload or {}).get("items", []),
            get_next_page_v2=_next_params_from_nextpagelink,
        )
    ],
)

for person in api.endpoint("people").items({"$top": 200}):
    ...
```

## SOAP example (Marshmallow + XPath)

```python
from marshmallow import Schema, fields, INCLUDE
from shared.api.client import ApiClient
from shared.api.endpoint import Endpoint
from shared.api.auth_strategy import NoAuth
from shared.api.codecs import SoapCodec
from shared.api.xml_utils import extractor_from_schema

class EmptySchema(Schema):
    class Meta:
        unknown = INCLUDE

class IdentitySchema(Schema):
    class Meta:
        unknown = INCLUDE
    def dump(self, obj, *, many=None):  # type: ignore[override]
        return obj or {}

class CountrySchema(Schema):
    ISOCode = fields.String(metadata={"xpath": ".//*[local-name()='sISOCode']"})

def build_envelope(params: dict) -> str:
    return f"""
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetCountry xmlns="http://example.com">
      <ISO>{params.get('ISO')}</ISO>
    </GetCountry>
  </soap:Body>
</soap:Envelope>
""".strip()

endpoint = Endpoint(
    name="GetCountry",
    path="https://example.com/Service.svc",
    method="POST",
    request_schema=IdentitySchema(),
    response_schema=EmptySchema(),
    codec=SoapCodec(
        action="http://example.com/Service.svc/GetCountry",
        envelope_builder=build_envelope,
    ),
    extract_items=extractor_from_schema(".//*[local-name()='GetCountryResult']", CountrySchema),
)
api = ApiClient(auth_strategy=NoAuth(), endpoints=[endpoint])
for row in api.endpoint("GetCountry").items({"ISO": "US"}):
    ...
```

## CSV example (download endpoints or inline CSV)

CsvCodec can parse CSV payloads. When `stream=True`, it returns an iterator under key `results_iter`, which the client will iterate without materializing the whole page when `Endpoint.materialize_pages=False`.

```python
from shared.api.codecs import CsvCodec
from shared.api.endpoint import Endpoint

def _download_url_path_builder(_base: str | None, fields: dict) -> str:
    return str(fields.get("url") or "")

download_csv = Endpoint(
    name="downloadCsv",
    path="",  # built dynamically from params
    method="GET",
    request_schema=IdentitySchema(),
    response_schema=EmptySchema(),
    codec=CsvCodec(stream=True),
    path_builder=_download_url_path_builder,
    materialize_pages=False,
)
```

## Parameters semantics and reserved keys

- Top-level keys you pass to `.pages(...)`/`.items(...)` are treated as request fields and validated/dumped via `request_schema`.
  - For GET endpoints: sent as query params
  - For non-GET endpoints: sent as JSON body
- Reserved keys (not part of request fields): `headers`, `timeout`
- Backwards compatibility: you may pass `query={...}` for GET or `json={...}` for non-GET; pagination hooks may return either a flat mapping or `{"query": ...}` / `{"json": ...}` to replace fields

## Pagination

- Prefer `get_next_page_v2(response, payload, params)` to compute the next request fields; it receives the parsed payload
- Legacy `get_next_page(response, params)` is still supported
- For nextPageLink-style APIs, return `{ "query": parsed_query }` to fully replace the query

## Codecs

- JsonCodec (default): JSON request/response
- SoapCodec: SOAP 1.1 with envelope builder and SOAPAction header, returns parsed XML root
- CsvCodec: parses CSV text; returns `{"results_iter": iterator}` when `stream=True`, else `{"results": list}`

## XML helpers

- `items_xpath_extractor(xpath, namespaces=None)`: return nodes via XPath for use with SoapCodec payloads
- `extractor_from_schema(items_xpath, Schema)`: build dict rows from schema fields with `metadata["xpath"]`
- `xml_items_to_dict_extractor(items_xpath, field_map)`: flexible mapping to dictionaries, including multi-value joins

## Auth strategies

- `BasicAuth(username, password, extra_headers=None)`
- `BearerAuth(token=..., token_getter=...)`
- `OAuth2RefreshTokenAuth(token_url, client_id, client_secret, refresh_token, ...)` – exchanges refresh token to bearer tokens, caches expiry, auto-retries once on 401

## Logging

Use a single-line structured format:

```
[connector] [service] event_type (k=v, k=v)
```

```python
from shared.logging import get_logger
LOG = get_logger("everyaction", "connector")
LOG.info("sync_start", mode="initial")
LOG.warning("rate_limited", attempt=2, delay_seconds=4)
```

## Marshmallow usage

- `request_schema.dump` maps params to payload/query
- `response_schema.load` validates/normalizes responses when JSON
- For pass-through, use `IdentitySchema` and `EmptySchema` patterns

## Endpoint reference (key fields)

```python
Endpoint(
    name: str,
    path: str,  # absolute or relative; use path_builder for dynamic URLs
    request_schema: Schema,
    response_schema: Schema,
    default_params: Mapping[str, Any] | None = None,
    method: Literal["GET","POST","PUT","PATCH","DELETE","HEAD","OPTIONS"] = "GET",
    get_next_page: Optional[Callable[[Response, Mapping[str, Any]], Mapping[str, Any] | None]] = None,
    get_next_page_v2: Optional[Callable[[Response, Any, Mapping[str, Any]], Mapping[str, Any] | None]] = None,
    parse_download_url: Optional[Callable[[Response], str | None]] = None,
    extract_items: Optional[Callable[[Any], Iterable[Any]]] = None,
    codec: PayloadCodec = JsonCodec(),
    path_builder: Optional[Callable[[str | None, Mapping[str, Any]], str]] = None,
    materialize_pages: bool = True,
)
```

Notes:
- ApiClient currently takes `auth_strategy`, `endpoints`, and an optional `session_factory`. Provide absolute URLs in `Endpoint.path` or use `path_builder` for dynamic targets.


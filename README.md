### Root Connectors Framework

This repository contains multiple example and production-grade connectors built with the Fivetran Connector SDK and a shared utilities library (`shared`). The shared library provides a small, composable HTTP client abstraction and helpers to accelerate REST/SOAP/CSV integrations.

Connectors live under `connectors/<name>` and generally expose the standard SDK `schema` and `update` functions. They depend on `shared` for HTTP and logging utilities.

#### Contents
- `shared/`: reusable HTTP client, codecs, XML helpers, logging
- `connectors/`: individual connectors (e.g., `weather`, `everyaction`, `sa360_custom_keywords`)

---

### Installation

Prerequisites:
- Python 3.11+
- A virtual environment (recommended)

Steps:
1. Create and activate a virtual environment
   - Windows PowerShell:
     - `python -m venv venv`
     - `venv\Scripts\Activate.ps1`
2. Install the Fivetran Connector SDK
   - `pip install fivetran-connector-sdk`
3. Install shared library (editable) and connector-specific deps (optional)
   - From repo root: `pip install -e ./shared`
   - For a given connector, install its `requirements.txt` in addition to the SDK and shared
     - e.g., `pip install -r connectors/everyaction/requirements.txt`

Notes:
- The `shared` package is a local editable install (import as `shared.*`).

---

### First-time setup

1. Choose a connector under `connectors/<name>`.
2. Review its `configuration.json` or README for required credentials and configuration fields.
3. Provide configuration to the SDK when running locally. You can:
   - Pass prompts interactively via `connector.debug()`
   - Or create an environment variable or local JSON file and load it in your connector entrypoint (pattern varies by connector)

Example: running a connector locally

```bash
cd connectors/weather
python connector.py  # or: python -m connectors.weather.connector
```

If the connector defines `connector = Connector(update=..., schema=...)` and a `__main__` with `connector.debug()`, this will start the local debug run.

---

### Basic usage pattern (building a connector)

At a high level:
1. Define your tables in `schema(configuration)` per the SDK spec
2. Implement `update(configuration, state)` to fetch and yield `Operations` (e.g., `op.upsert("table", row)` and `op.checkpoint(state)`) 
3. Use `shared` to make API calls and log events

Minimal example using `shared`:

```python
from fivetran_connector_sdk import Connector, Operations as op
from marshmallow import Schema, INCLUDE

from shared.api.client import ApiClient
from shared.api.endpoint import Endpoint
from shared.api.auth_strategy import BasicAuth
from shared.logging import get_logger

class EmptySchema(Schema):
    class Meta:
        unknown = INCLUDE

class IdentitySchema(Schema):
    class Meta:
        unknown = INCLUDE
    def dump(self, obj, *, many=None):  # type: ignore[override]
        return obj or {}

LOG = get_logger("my_connector", "connector")

def schema(configuration: dict):
    return [
        {
            "table": "items",
            "primary_key": ["id"],
            "columns": {"id": "string", "name": "string"},
        }
    ]

def build_api(configuration: dict) -> ApiClient:
    auth = BasicAuth(
        username=configuration["username"],
        password=configuration["password"],
        extra_headers={"Accept": "application/json"},
    )
    endpoint = Endpoint(
        name="items",
        path="https://api.example.com/v1/items",
        method="GET",
        request_schema=IdentitySchema(),
        response_schema=EmptySchema(),
        extract_items=lambda payload: (payload or {}).get("items", []),
    )
    return ApiClient(auth_strategy=auth, endpoints=[endpoint])

def update(configuration: dict, state: dict):
    api = build_api(configuration)
    for item in api.endpoint("items").items({"limit": 100}):
        yield op.upsert("items", item)
    yield op.checkpoint(state or {})

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
```

---

### Deploying

- Local debug: `python connector.py` inside a connector directory
- SDK CLI (optional): `fivetran debug` and `fivetran deploy` per the SDK docs 
- Follow the SDK docs for authenticating and deploying to your Fivetran destination

---

### Troubleshooting

- Ensure `shared` is installed in your environment: `python -c "import shared; print(shared.__file__)"`
- Verify credentials and required config fields match the connector's `configuration.json`
- Use `shared.logging.get_logger` for structured logs and check output for high-signal events

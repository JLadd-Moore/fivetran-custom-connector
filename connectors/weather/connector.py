from datetime import datetime

import requests as rq
from marshmallow import Schema, fields, INCLUDE

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from shared.api.client import ApiClient
from shared.api.auth_strategy import NoAuth
from shared.api.endpoint import Endpoint, items_path_extractor


class EmptyRequestSchema(Schema):
    class Meta:
        unknown = INCLUDE


class ForecastResponseSchema(Schema):
    properties = fields.Dict(required=True)

    class Meta:
        unknown = INCLUDE


class PeriodSchema(Schema):
    number = fields.Integer(required=True)
    name = fields.String(required=True)
    startTime = fields.DateTime(required=True)
    endTime = fields.DateTime(required=True)
    isDaytime = fields.Boolean(required=True)
    temperature = fields.Integer(required=True)
    temperatureUnit = fields.String(required=True)
    temperatureTrend = fields.String(required=False, allow_none=True)
    windSpeed = fields.String(required=False)
    windDirection = fields.String(required=False)
    icon = fields.String(required=False)
    shortForecast = fields.String(required=False)
    detailedForecast = fields.String(required=False)


def schema(configuration: dict):
    return [
        {
            "table": "period",  # Name of the table in the destination.
            "primary_key": ["startTime"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "name": "STRING",  # String column for the period name.
                "startTime": "UTC_DATETIME",  # UTC date-time column for the start time.
                "endTime": "UTC_DATETIME",  # UTC date-time column for the end time.
                "temperature": "INT",
            },
        }
    ]


def update(configuration: dict, state: dict):
    log.warning("Example: QuickStart Examples - Weather")

    cursor = state["startTime"] if "startTime" in state else "0001-01-01T00:00:00Z"

    forecast_endpoint = endpoint = Endpoint(
        name="forecast",
        path="https://api.weather.gov/gridpoints/ILM/58,40/forecast",
        method="GET",
        request_schema=EmptyRequestSchema(),
        response_schema=ForecastResponseSchema(),
        extract_items=items_path_extractor("properties.periods"),
    )
    api = ApiClient(auth_strategy=NoAuth(), endpoints=[endpoint])

    for period in api.endpoint("forecast").items({}):
        cursor = period.get("startTime")
        yield op.upsert("period", period)

    yield op.checkpoint({"startTime": cursor})


connector = Connector(update=update, schema=schema)

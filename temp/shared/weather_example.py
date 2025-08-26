from __future__ import annotations

from typing import Any

from marshmallow import Schema, fields, INCLUDE

from .api import ApiClient
from .auth_strategy import NoAuth
from .endpoint import Endpoint, items_path_extractor


class EmptyRequestSchema(Schema):
    class Meta:
        unknown = INCLUDE


class ForecastResponseSchema(Schema):
    # Only include the minimal fields we care about for the example
    properties = fields.Dict(required=True)

    class Meta:
        unknown = INCLUDE


# the actual object we want to extract
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


if __name__ == "__main__":

    forecast_endpoint = endpoint = Endpoint(
        name="forecast",
        path="https://api.weather.gov/gridpoints/ILM/58,40/forecast",
        method="GET",
        request_schema=EmptyRequestSchema(),
        response_schema=ForecastResponseSchema(),
        extract_items=items_path_extractor("properties.periods"),
    )
    api = ApiClient(base_url=None, auth_strategy=NoAuth(), endpoints=[endpoint])

    print("Fetching forecast from NWS...")
    for page in api.endpoint("forecast").get():
        print(f"Received {len(page)} periods")
        for period in list(page)[:3]:
            print(
                {
                    "name": period.get("name"),
                    "startTime": period.get("startTime"),
                    "endTime": period.get("endTime"),
                    "temperature": period.get("temperature"),
                }
            )
    print("Done.")

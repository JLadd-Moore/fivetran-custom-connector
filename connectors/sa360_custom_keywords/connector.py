# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows the use of a requirements.txt file and a connector that calls a publicly available API to get the weather forecast data for Myrtle Beach in South Carolina, USA.
# It also shows how to use the logging functionality provided by fivetran_connector_sdk, by logging important steps using log.info() and log.fine()
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

from datetime import datetime

import requests as rq

from fivetran_connector_sdk import (
    Connector,
)
from fivetran_connector_sdk import (
    Logging as log,
)
from fivetran_connector_sdk import (
    Operations as op,
)

from shared.api.client import ApiClient
from shared.api.auth_strategy import OAuth2RefreshTokenAuth
from shared.api.endpoint import Endpoint, items_path_extractor
from marshmallow import Schema, fields, INCLUDE


class SA360SearchRequestSchema(Schema):
    query = fields.String(required=True)
    pageSize = fields.Integer(required=False)
    pageToken = fields.String(required=False, allow_none=True)

    class Meta:
        unknown = INCLUDE


class SA360SearchResponseSchema(Schema):
    results = fields.List(fields.Dict(), required=False)
    nextPageToken = fields.String(required=False)

    class Meta:
        unknown = INCLUDE


class EmptyRequestSchema(Schema):
    class Meta:
        unknown = INCLUDE


class CustomColumnsListResponseSchema(Schema):
    customColumns = fields.List(fields.Dict(), required=False)

    class Meta:
        unknown = INCLUDE


# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
def schema(configuration: dict):
    return [
        {
            "table": "custom_column_metrics",
            "primary_key": [
                "column_id",
                "customer_id",
                "campaign_id",
                "adgroup_id",
                "date",
                "adgroup_criterion_id",
                "keyword_text",
                "keyword_match_type",
            ],
            "columns": {
                "date": "NAIVE_DATE",
                "column_id": "STRING",
                "campaign_id": "STRING",
                "customer_id": "STRING",
                "adgroup_id": "STRING",
                "adgroup_name": "STRING",
                "value": "STRING",
                "adgroup_criterion_id": "STRING",
                "keyword_text": "STRING",
                "keyword_match_type": "STRING",
                "campaign_name": "STRING",
                "account_name": "STRING",
                "currency_code": "STRING",
                "clicks": "STRING",
                "impressions": "STRING",
                "cost": "STRING",
            },
        },
    ]


def get_customer_clients(api: ApiClient, submanager_account_id: str):

    customer_clients_query = "SELECT customer_client.id FROM customer_client"
    for customer_client in api.endpoint("sa360_search").items(
        {"query": customer_clients_query}
    ):
        yield customer_client


# def get_submanger_api(auth, submanager_account_id: str):
#     search_endpoint = Endpoint(
#         name="sa360_search",
#         path=f"https://searchads360.googleapis.com/v0/customers/{submanager_account_id}/searchAds360:search",
#         method="POST",
#         request_schema=SA360SearchRequestSchema(),
#         response_schema=SA360SearchResponseSchema(),
#         extract_items=items_path_extractor("results"),
#         get_next_page=lambda response, params: (
#             {
#                 **(params or {}),
#                 "pageToken": response.json().get("nextPageToken"),
#             }
#             if response.json().get("nextPageToken")
#             else None
#         ),
#     )

#     custom_columns_endpoint = Endpoint(
#         name="sa360_custom_columns",
#         path=f"https://searchads360.googleapis.com/v0/customers/{submanager_account_id}/customColumns",
#         method="GET",
#         request_schema=EmptyRequestSchema(),
#         response_schema=CustomColumnsListResponseSchema(),
#         extract_items=items_path_extractor("customColumns"),
#     )

#     api = ApiClient(
#         auth_strategy=auth, endpoints=[search_endpoint, custom_columns_endpoint]
#     )
#     return api


def get_account_api(auth, account_id: str):
    search_endpoint = Endpoint(
        name="sa360_search",
        path=f"https://searchads360.googleapis.com/v0/customers/{account_id}/searchAds360:search",
        method="POST",
        request_schema=SA360SearchRequestSchema(),
        response_schema=SA360SearchResponseSchema(),
        extract_items=items_path_extractor("results"),
        get_next_page=lambda response, params: (
            {
                **(params or {}),
                "pageToken": response.json().get("nextPageToken"),
            }
            if response.json().get("nextPageToken")
            else None
        ),
    )

    custom_columns_endpoint = Endpoint(
        name="sa360_custom_columns",
        path=f"https://searchads360.googleapis.com/v0/customers/{account_id}/customColumns",
        method="GET",
        request_schema=EmptyRequestSchema(),
        response_schema=CustomColumnsListResponseSchema(),
        extract_items=items_path_extractor("customColumns"),
    )

    def _extract_rows(payload):
        return generate_custom_column_rows_from_page(payload, account_id)

    custom_column_values_endpoint = Endpoint(
        name="custom_column_values",
        path=f"https://searchads360.googleapis.com/v0/customers/{account_id}/searchAds360:search",
        method="POST",
        request_schema=SA360SearchRequestSchema(),
        response_schema=SA360SearchResponseSchema(),
        extract_items=_extract_rows,
        get_next_page=lambda response, params: (
            {
                **(params or {}),
                "pageToken": response.json().get("nextPageToken"),
            }
            if response.json().get("nextPageToken")
            else None
        ),
    )

    return ApiClient(
        auth_strategy=auth,
        endpoints=[
            search_endpoint,
            custom_columns_endpoint,
            custom_column_values_endpoint,
        ],
    )


def sa360_custom_columns_query(
    custom_columns: list[dict], start_date: str | None
) -> str:
    from datetime import datetime as _dt

    custom_columns_select = ", ".join(
        [f"custom_columns.id[{cid.get('id')}]" for cid in custom_columns]
    )

    start = "2023-01-01" if not start_date else start_date
    today = _dt.now().date().isoformat()
    select_custom = f", {custom_columns_select}" if custom_columns_select else ""
    return (
        "SELECT "
        "campaign.id, campaign.name, "
        "ad_group.id, ad_group.name, ad_group_criterion.criterion_id, "
        "ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, "
        "metrics.clicks, metrics.impressions, metrics.cost_micros, "
        "customer.currency_code, customer.descriptive_name, segments.date"
        f"{select_custom} "
        "FROM keyword_view "
        f"WHERE segments.date BETWEEN '{start}' AND '{today}' "
        "ORDER BY segments.date ASC"
    )


def generate_custom_column_rows_from_page(page: dict, customer_id: str):
    results = page.get("results", [])
    column_headers = page.get("customColumnHeaders", [])
    for record in results:
        campaign_id = record.get("campaign", {}).get("id")
        campaign_name = record.get("campaign", {}).get("name")
        adgroup_id = record.get("adGroup", {}).get("id")
        adgroup_name = record.get("adGroup", {}).get("name")
        clicks = record.get("metrics", {}).get("clicks", "0")
        impressions = record.get("metrics", {}).get("impressions", "0")
        cost = record.get("metrics", {}).get("costMicros", "0")
        adgroup_criterion_id = record.get("adGroupCriterion", {}).get(
            "criterionId", "0"
        )
        kw_text = record.get("adGroupCriterion", {}).get("keyword", {}).get("text", "")
        kw_match_type = (
            record.get("adGroupCriterion", {}).get("keyword", {}).get("matchType", "")
        )
        account_name = record.get("customer", {}).get("descriptiveName")
        currency = record.get("customer", {}).get("currencyCode")
        date = record.get("segments", {}).get("date")
        custom_cols = record.get("customColumns", [])

        for col_val, col_header in zip(custom_cols, column_headers):
            val = col_val.get("doubleValue")
            column_id = col_header.get("id")
            yield {
                "column_id": column_id,
                "value": val,
                "date": date,
                "campaign_id": campaign_id,
                "adgroup_id": adgroup_id,
                "adgroup_name": adgroup_name,
                "customer_id": customer_id,
                "adgroup_criterion_id": adgroup_criterion_id,
                "keyword_text": kw_text,
                "keyword_match_type": kw_match_type,
                "campaign_name": campaign_name,
                "account_name": account_name,
                "currency_code": currency,
                "clicks": clicks,
                "impressions": impressions,
                "cost": cost,
            }


def update(configuration: dict, state: dict):
    log.warning("SA360: Hello World - customer_client list via search API")
    if not state:
        state = {}
    iterative_date_cursor = state.get("iterative_date_cursor")
    date_cursor = state.get("date_cursor", iterative_date_cursor)
    sub_cursor = state.get("sub_cursor")
    customer_cursor = state.get("customer_cursor")

    submanager_account_ids = configuration["submanager_account_ids"].split(",")

    # Build OAuth2 refreshable auth for SA360
    auth = OAuth2RefreshTokenAuth(
        token_url="https://oauth2.googleapis.com/token",
        client_id=configuration["google_client_id"],
        client_secret=configuration["google_client_secret"],
        refresh_token=configuration["google_refresh_token"],
        extra_headers={
            "login-customer-id": configuration["google_login_customer_id"],
        },
    )

    for submanager_account_id in submanager_account_ids:

        # if we have cursor, that means we are resuming from a checkpoint
        # skip already processed submanager accounts
        if sub_cursor and sub_cursor != submanager_account_id:
            continue
        else:
            state["sub_cursor"] = submanager_account_id
            yield op.checkpoint(state or {})

        sub_api = get_account_api(auth, submanager_account_id)
        log.info(f"Processing submanager account {submanager_account_id}")

        customer_clients_query = "SELECT customer_client.id FROM customer_client"
        for customer_client in sub_api.endpoint("sa360_search").items(
            {"query": customer_clients_query}
        ):

            # if we have cursor, that means we are resuming from a checkpoint
            # skip already processed customers
            customer_id = customer_client.get("customerClient", {}).get("id")
            if customer_cursor and customer_cursor != customer_id:
                log.info(f"Skipping customer client {customer_id}")
                continue
            elif not customer_id:
                log.info(f"No customer ID found for customer client {customer_client}")
                continue
            elif customer_id == submanager_account_id:
                log.info(f"Skipping submanager account {customer_id}")
                continue
            else:
                log.info(f"Processing customer client ID: {customer_id}")
                state["customer_cursor"] = customer_id
                yield op.checkpoint(state or {})

            customer_api = get_account_api(auth, customer_id)

            # get custom columns for client and build search api query
            custom_columns = list(
                customer_api.endpoint("sa360_custom_columns").items({})
            )
            query = sa360_custom_columns_query(custom_columns, "2025-06-01")
            log.info(f"Query: {query}")
            # iterate through metrics and upsert to custom_column_metrics table
            for metric_index, metric in enumerate(
                customer_api.endpoint("custom_column_values").items(
                    {"query": query, "pageSize": 10}
                )
            ):
                # logs for visibility
                log.info(
                    f"Inserted {metric_index} rows for customer client {customer_id}"
                )
                if metric_index > 0 and metric_index % 1000 == 0:
                    log.info(
                        f"Processed {metric_index} metrics for customer client {customer_id}"
                    )

                # checkpoint for if sync fails, we can resume from recent data
                # if metric_index > 0 and metric_index % 10000 == 0:
                #     state["date_cursor"] = metric.get("date")
                #     yield op.checkpoint(state or {})
                yield op.upsert("custom_column_metrics", metric)
            # state["date_cursor"] = date_cursor

    log.info(
        f"Processed all data, setting iterative date cursor to {datetime.now().isoformat()}"
    )
    state["iterative_date_cursor"] = datetime.now().isoformat()
    yield op.checkpoint(state or {})


connector = Connector(update=update, schema=schema)

from datetime import datetime
import time
from urllib.parse import urlparse, parse_qs
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op

from marshmallow import Schema, INCLUDE

from shared.api.client import ApiClient
from shared.api.endpoint import Endpoint
from shared.api.codecs import CsvCodec
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


LOG = get_logger("everyaction", "connector")


# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
def schema(configuration: dict):
    return [
        {
            "table": "people",
            "primary_key": ["vanId"],
            "columns": {
                "vanId": "string",
                "firstName": "string",
                "lastName": "string",
                "middleName": "string",
                "suffix": "string",
                "title": "string",
                "sourceFileTitle": "string",
                "sourceFileFirstName": "string",
                "sourceFileMiddleName": "string",
                "sourceFileLastName": "string",
                "sourceFileSuffix": "string",
                "contactSource": "string",
                "contactMode": "string",
                "organizationContactCommonName": "string",
                "organizationContactOfficialName": "string",
                "salutation": "string",
                "formalSalutation": "string",
                "additionalSalutation": "string",
                "preferredPronoun": "string",
                "pronouns": "string",
                "namePronunciation": "string",
                "envelopeName": "string",
                "formalEnvelopeName": "string",
                "additionalEnvelopeName": "string",
                "contactMethodPreferenceCode": "string",
                "contactMethodPreferenceId": "string",
                "contactMethodPreferenceName": "string",
                "assistantName": "string",
                "nickname": "string",
                "website": "string",
                "professionalSuffix": "string",
                "party": "string",
                "employer": "string",
                "occupation": "string",
                "jobTitle": "string",
                "sex": "string",
                "dateOfBirth": "string",
                "isDeceased": "string",
                "dateCreated": "string",
                "dateAcquired": "string",
                "selfReportedRace": "string",
                "selfReportedEthnicity": "string",
                "selfReportedRaces": "string",
                "selfReportedEthnicities": "string",
                "selfReportedGenders": "string",
                "selfReportedSexualOrientations": "string",
                "selfReportedLanguagePreference": "string",
                "selfReportedLanguagePreferences": "string",
                "emails": "string",
                "phones": "string",
                "addresses": "string",
                "recordedAddresses": "string",
                "pollingLocation": "string",
                "earlyVoteLocation": "string",
                "identifiers": "string",
                "codes": "string",
                "customFields": "string",
                "primaryCustomField": "string",
                "contributionSummary": "string",
                "suppressions": "string",
                "caseworkCases": "string",
                "caseworkIssues": "string",
                "caseworkStories": "string",
                "notes": "string",
                "scores": "string",
                "customProperties": "string",
                "electionRecords": "string",
                "membershipStatus": "string",
                "organizationRoles": "string",
                "districts": "string",
                "surveyQuestionResponses": "string",
                "finderNumber": "string",
                "biographyImageUrl": "string",
                "primaryContact": "string",
                "state_code": "string",
            },
        },
        {
            "table": "contributions",
            "primary_key": ["contactsContributionId"],
            "columns": {
                "contactsContributionId": "string",
                "vanId": "string",
                "donorName": "string",
                "donorNameFirstLast": "string",
                "designationId": "string",
                "designationName": "string",
                "dateReceived": "string",
                "amount": "string",
                "amountRemaining": "string",
                "amountRefunded": "string",
                "period": "string",
                "cycle": "string",
                "sourceCodeId": "string",
                "sourceCodeName": "string",
                "sourceCodeDescription": "string",
                "sourceCodeStaticPath": "string",
                "type": "string",
                "van_id": "string",
            },
        },
    ]


# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
def update(configuration: dict, state: dict):
    """
    Main update function that handles both initial sync and incremental sync.

    Initial sync: Fetches all people for all states, then their contributions
    Incremental sync: Uses Changed Entity Export for efficient updates (legacy path)
    """
    api = build_api_client(configuration)

    # Check if this is initial sync or incremental sync
    initial_sync_complete = state.get("initial_sync_complete", False)

    if not initial_sync_complete:
        LOG.info("sync_start", mode="initial")
        yield from perform_initial_sync(configuration, api, state)
    else:
        LOG.info("sync_start", mode="incremental")
        yield from perform_incremental_sync(configuration, api, state)


def _next_params_from_nextpagelink(_: object, page_payload: dict, __: dict):
    """Parse EveryAction nextPageLink URL into replacement query params."""
    if not isinstance(page_payload, dict):
        return None
    next_link = page_payload.get("nextPageLink")
    if not next_link:
        return None
    parsed = urlparse(next_link)
    qs = parse_qs(parsed.query)
    flat = {k: (v[0] if isinstance(v, list) and v else v) for k, v in qs.items()}
    return {"query": flat}


def build_api_client(configuration: dict) -> ApiClient:
    username = configuration["username"]
    password = configuration["password"]
    auth = BasicAuth(
        username=username,
        password=password,
        extra_headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    people_endpoint = Endpoint(
        name="people",
        path="https://api.securevan.com/v4/people",
        method="GET",
        request_schema=IdentitySchema(),
        response_schema=EmptySchema(),
        extract_items=lambda payload: (payload or {}).get("items", []),
        get_next_page_v2=_next_params_from_nextpagelink,
    )

    contributions_endpoint = Endpoint(
        name="recentContributions",
        path="https://api.securevan.com/v4/contributions/recentContributions",
        method="GET",
        request_schema=IdentitySchema(),
        response_schema=EmptySchema(),
        extract_items=lambda payload: (payload or {}).get("items", []),
        get_next_page_v2=_next_params_from_nextpagelink,
    )

    # Incremental export endpoints (create job, poll status, download csv)
    create_export_endpoint = Endpoint(
        name="createExportJob",
        path="https://api.securevan.com/v4/changedEntityExportJobs",
        method="POST",
        request_schema=IdentitySchema(),
        response_schema=EmptySchema(),
    )

    def _job_status_path_builder(_base: str | None, fields: dict) -> str:
        export_job_id = fields.get("exportJobId") or fields.get("id")
        return (
            f"https://api.securevan.com:443/v4/changedEntityExportJobs/{export_job_id}"
        )

    job_status_endpoint = Endpoint(
        name="jobStatus",
        path="https://api.securevan.com:443/v4/changedEntityExportJobs",
        method="GET",
        request_schema=IdentitySchema(),
        response_schema=EmptySchema(),
        path_builder=_job_status_path_builder,
    )

    def _download_path_builder(_base: str | None, fields: dict) -> str:
        return str(fields.get("url") or "")

    download_csv_endpoint = Endpoint(
        name="downloadCsv",
        path="",
        method="GET",
        request_schema=IdentitySchema(),
        response_schema=EmptySchema(),
        codec=CsvCodec(stream=True),
        path_builder=_download_path_builder,
        materialize_pages=False,
    )

    return ApiClient(
        auth_strategy=auth,
        endpoints=[
            people_endpoint,
            contributions_endpoint,
            create_export_endpoint,
            job_status_endpoint,
            download_csv_endpoint,
        ],
    )


def perform_initial_sync(configuration: dict, api, state: dict):
    """
    Performs initial sync by iterating through all states and fetching all people/contributions.
    Uses state cursor to track progress across sync runs.
    Processes people and contributions page by page for memory efficiency.
    """
    from state_codes import US_STATE_CODES

    # Get current state cursor or start from beginning
    current_state_index = state.get("state_cursor_index", 0)

    # Process states starting from cursor position
    for i in range(current_state_index, len(US_STATE_CODES)):
        state_code = US_STATE_CODES[i]
        LOG.info(
            "state_start", index=i + 1, total=len(US_STATE_CODES), state=state_code
        )

        # Process people page by page for memory efficiency
        page_count = 0
        total_people_count = 0
        total_contributions_count = 0

        for page_items in api.endpoint("people").pages(
            {"stateOrProvince": state_code, "$top": 200}
        ):
            page_count += 1
            LOG.info("page_start", state=state_code, page=page_count)

            items = list(page_items or [])
            if not items:
                LOG.info("page_empty", state=state_code, page=page_count)
                continue

            # Process people from this page
            people_in_page = []
            van_ids_in_page = []

            for person in items:
                # Parse numeric fields and add state tracking
                person = transform_person_data(person, state_code)
                people_in_page.append(person)
                if person.get("vanId"):
                    van_ids_in_page.append(person["vanId"])

            LOG.info(
                "people_fetched",
                state=state_code,
                page=page_count,
                count=len(people_in_page),
            )

            # Upsert people data for this page
            if people_in_page:
                for person in people_in_page:
                    yield op.upsert("people", person)
                total_people_count += len(people_in_page)
                LOG.info(
                    "people_upserted",
                    state=state_code,
                    page=page_count,
                    count=len(people_in_page),
                )

            # Get contributions for people in this page
            contributions_in_page = 0
            for van_id in van_ids_in_page:
                for contribution_items in api.endpoint("recentContributions").pages(
                    {"vanId": van_id, "$top": 200}
                ):
                    for contribution in list(contribution_items or []):
                        contribution = transform_contribution_data(contribution, van_id)
                        yield op.upsert("contributions", contribution)
                        contributions_in_page += 1

            total_contributions_count += contributions_in_page
            LOG.info(
                "contributions_upserted",
                state=state_code,
                page=page_count,
                count=contributions_in_page,
            )

            # Debug: break after first page for testing
            # break

        LOG.info(
            "state_complete",
            state=state_code,
            people=total_people_count,
            contributions=total_contributions_count,
        )

        # Checkpoint progress after each state
        yield op.checkpoint(
            {"state_cursor_index": i + 1, "initial_sync_complete": False}
        )
        # remove before pushing
        # break

    # Initial sync complete - mark as done and set timestamp for incremental syncs
    current_timestamp = datetime.utcnow().isoformat() + "Z"
    yield op.checkpoint(
        {
            "initial_sync_complete": True,
            "last_sync_timestamp": current_timestamp,
            "state_cursor_index": None,  # Clear cursor
        }
    )
    LOG.info("sync_complete", mode="initial")


def perform_incremental_sync(configuration: dict, api, state: dict):
    """Incremental sync using Changed Entity Export via ApiClient endpoints."""
    last_sync_timestamp = state.get("last_sync_timestamp")
    if not last_sync_timestamp:
        LOG.warning("incremental_missing_cursor")
        return perform_initial_sync(configuration, api, {})

    LOG.info("incremental_start", since=last_sync_timestamp)

    def _poll_job(export_job_id: int) -> dict:
        while True:
            status = next(
                api.endpoint("jobStatus").items({"exportJobId": export_job_id})
            )
            job_status = status.get("jobStatus")
            if job_status == "Complete":
                return status
            if job_status in {"Failed", "Cancelled", "Error"}:
                raise RuntimeError(f"Export job {export_job_id} failed: {status}")
            LOG.debug("export_status", export_job_id=export_job_id, status=job_status)
            time.sleep(10)

    def _run_export(resource_type: str, row_mapper):
        payload = {
            "resourceType": resource_type,
            "dateChangedFrom": last_sync_timestamp,
            "includeInactive": False,
            "fileSizeKbLimit": 40000,
        }
        job_meta = next(api.endpoint("createExportJob").items(payload))
        export_job_id = job_meta.get("exportJobId")
        LOG.info(
            "export_created", export_job_id=export_job_id, resource_type=resource_type
        )
        completed = _poll_job(export_job_id)
        files = completed.get("files", []) or []
        processed = 0
        for f in files:
            url = f.get("downloadUrl")
            if not url:
                continue
            for row in api.endpoint("downloadCsv").items({"url": url}):
                mapped = row_mapper(row)
                if not mapped:
                    continue
                yield mapped
                processed += 1
        LOG.info(
            "export_records_processed", resource_type=resource_type, count=processed
        )

    # Contacts
            contacts_count = 0
    for person in _run_export("Contacts", transform_csv_contact_to_person):
        yield op.upsert("people", person)
        contacts_count += 1
    LOG.info("contacts_processed", count=contacts_count)

    # Contributions
        contributions_count = 0
    for contribution in _run_export(
        "Contributions", transform_csv_contribution_to_contribution
            ):
                    yield op.upsert("contributions", contribution)
                    contributions_count += 1
    LOG.info("contributions_processed", count=contributions_count)

        if contacts_count == 0 and contributions_count == 0:
        LOG.warning("incremental_no_data")

    current_timestamp = datetime.utcnow().isoformat() + "Z"
    yield op.checkpoint(
        {"initial_sync_complete": True, "last_sync_timestamp": current_timestamp}
    )


def transform_person_data(person: dict, state_code: str) -> dict:
    """Transform person data from API format to schema format."""
    # Parse numeric fields
    if person.get("vanId"):
        person["vanId"] = int(person["vanId"])
    if person.get("contactMethodPreferenceId"):
        person["contactMethodPreferenceId"] = int(person["contactMethodPreferenceId"])

    # Add state_code to each person record
    person["state_code"] = state_code
    return person


def transform_contribution_data(contribution: dict, van_id: int) -> dict:
    """Transform contribution data from API format to schema format."""
    # Parse numeric fields
    if contribution.get("contactsContributionId"):
        contribution["contactsContributionId"] = int(
            contribution["contactsContributionId"]
        )
    if contribution.get("vanId"):
        contribution["vanId"] = int(contribution["vanId"])
    if contribution.get("designationId"):
        contribution["designationId"] = int(contribution["designationId"])
    if contribution.get("sourceCodeId"):
        contribution["sourceCodeId"] = int(contribution["sourceCodeId"])

    # Add van_id field for tracking
    contribution["van_id"] = str(van_id)
    return contribution


def transform_csv_contact_to_person(csv_row: dict) -> dict:
    """Transform CSV contact row to person schema format."""
    # Skip records with errors
    if csv_row.get("ErrorMessage"):
        LOG.warning("skip_contact_error", error=csv_row.get("ErrorMessage"))
        return None

    van_id = csv_row.get("VanID")
    if not van_id:
        return None

    # Map CSV fields to schema fields
    person = {
        "vanId": int(van_id),
        "firstName": csv_row.get("FirstName", ""),
        "lastName": csv_row.get("LastName", ""),
        "middleName": csv_row.get("MiddleName", ""),
        # Map other fields as needed based on CSV structure
        "emails": csv_row.get("Email", ""),
        "phones": csv_row.get("Phone", ""),
        "addresses": f"{csv_row.get('StreetAddress', '')} {csv_row.get('City', '')} {csv_row.get('State', '')} {csv_row.get('ZipOrPostal', '')}".strip(),
        "state_code": csv_row.get("State", ""),
    }

    return person


def transform_csv_contribution_to_contribution(csv_row: dict) -> dict:
    """Transform CSV contribution row to contribution schema format."""
    # Skip records with errors
    if csv_row.get("ErrorMessage"):
        LOG.warning("skip_contribution_error", error=csv_row.get("ErrorMessage"))
        return None

    contribution_id = csv_row.get("ContributionID")
    van_id = csv_row.get("VanID")

    if not contribution_id or not van_id:
        return None

    # Map CSV fields to schema fields
    contribution = {
        "contactsContributionId": int(contribution_id),
        "vanId": int(van_id),
        "van_id": van_id,
        "amount": csv_row.get("Amount", ""),
        "amountRemaining": csv_row.get("AmountRemaining", ""),
        "dateReceived": csv_row.get("DateReceived", ""),
        "designationName": csv_row.get("Designation", ""),
        "sourceCodeName": csv_row.get("SourceCode", ""),
        # Map other fields as available in CSV
    }

    return contribution


connector = Connector(update=update, schema=schema)
if __name__ == "__main__":
    connector.debug()

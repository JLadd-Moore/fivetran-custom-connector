import requests as rq
import time
from typing import Generator, Dict, List, Any
from fivetran_connector_sdk import Logging as log
from shared.logging import get_logger
from state_codes import US_STATE_CODES


LOG = get_logger("everyaction", "everyaction_api")


def make_everyaction_request(
    config: dict,
    method: str,
    url: str,
    session: rq.Session,
    **kwargs,
) -> rq.Response:
    """
    Makes a request to the EveryAction API with basic authentication.
    Handles rate limiting and retries with exponential backoff.
    """
    rate_limit_retries = 5
    backoff_factor = 2

    while True:
        response = session.request(method, url, **kwargs)

        # Check for rate limit error: retry with exponential backoff
        if response.status_code == 429 and rate_limit_retries > 0:
            attempt = 6 - rate_limit_retries
            delay = backoff_factor ** (attempt - 1)
            LOG.info("rate_limited", attempt=attempt, delay_seconds=delay)
            time.sleep(delay)
            rate_limit_retries -= 1
            continue

        try:
            response.raise_for_status()
        except rq.exceptions.HTTPError as e:
            LOG.error("http_error", status=response.status_code, url=url, error=str(e))
            raise

        return response


def get_everyaction_session(configuration: dict) -> rq.Session:
    """
    Creates a requests.Session with basic authentication for EveryAction API.
    """
    username = configuration["username"]
    password = configuration["password"]

    session = rq.Session()
    session.auth = (username, password)
    session.headers.update(
        {"Content-Type": "application/json", "Accept": "application/json"}
    )
    return session


def get_paginated_data(
    config: dict, session: rq.Session, base_url: str, params: dict = None
) -> Generator[Dict[str, Any], None, None]:
    """
    Generic method to handle pagination for EveryAction API endpoints.
    Yields each page of results.

    Args:
        config: Configuration dictionary
        session: Requests session with authentication
        base_url: Base URL for the endpoint
        params: Query parameters to include in the request
    """
    if params is None:
        params = {}

    current_url = base_url
    page_count = 0

    while current_url:
        LOG.debug("page_fetch", page=page_count + 1)

        # For the first page, use the provided params
        # For subsequent pages, use the nextPageLink which already includes all parameters
        if page_count == 0:
            response = make_everyaction_request(
                config, method="GET", url=current_url, session=session, params=params
            )
        else:
            # For subsequent pages, the nextPageLink already contains all necessary parameters
            response = make_everyaction_request(
                config, method="GET", url=current_url, session=session
            )

        json_data = response.json()
        yield json_data

        # Get next page URL if available
        current_url = json_data.get("nextPageLink")
        page_count += 1


def get_paginated_data_manual_url(
    config: dict, session: rq.Session, url: str
) -> Generator[Dict[str, Any], None, None]:
    """Deprecated: use ApiClient/Endpoint."""
    current_url = url
    page_count = 0
    while current_url:
        LOG.debug("page_fetch", page=page_count + 1)
        response = make_everyaction_request(
            config, method="GET", url=current_url, session=session
        )
        json_data = response.json()
        yield json_data
        current_url = json_data.get("nextPageLink")
        page_count += 1


def get_people_by_state(
    config: dict, session: rq.Session, state_code: str, batch_size: int = None
) -> Generator[Dict[str, Any], None, None]:
    """Deprecated: use ApiClient/Endpoint."""
    base_url = "https://api.securevan.com/v4/people"
    url = f"{base_url}?stateOrProvince={state_code}"
    if batch_size:
        url += f"&$top={batch_size}"
    yield from get_paginated_data_manual_url(config, session, url)


def get_contributions_by_van_id(
    config: dict, session: rq.Session, van_id: int, batch_size: int = None
) -> Generator[Dict[str, Any], None, None]:
    """Deprecated: use ApiClient/Endpoint."""
    base_url = f"https://api.securevan.com/v4/contributions/recentContributions"
    params = {"vanId": van_id}
    if batch_size:
        params["$top"] = batch_size
    current_url = base_url
    if params:
        current_url += "?" + "&".join([f"{k}={v}" for k, v in params.items()])
    yield from get_paginated_data_manual_url(config, session, current_url)


# Changed Entity Export API functions
import csv
import io
import requests as rq
from datetime import datetime, timedelta


def create_changed_entity_export_job(
    config: dict,
    session: rq.Session,
    resource_type: str,
    date_changed_from: str,
    date_changed_to: str = None,
    requested_fields: List[str] = None,
    include_inactive: bool = False,
) -> dict:
    """
    Creates a Changed Entity Export Job and returns the job metadata.

    Args:
        config: Configuration dictionary
        session: Requests session with authentication
        resource_type: Type of resource (e.g., "Contacts", "Contributions")
        date_changed_from: ISO datetime string for start of change window
        date_changed_to: ISO datetime string for end of change window (optional)
        requested_fields: List of specific fields to include (optional)
        include_inactive: Whether to include inactive records

    Returns:
        Export job metadata dictionary
    """
    url = "https://api.securevan.com/v4/changedEntityExportJobs"

    payload = {
        "resourceType": resource_type,
        "dateChangedFrom": date_changed_from,
        "includeInactive": include_inactive,
        "fileSizeKbLimit": 40000,  # 40MB limit
    }

    if date_changed_to:
        payload["dateChangedTo"] = date_changed_to

    if requested_fields:
        payload["requestedFields"] = requested_fields

    response = make_everyaction_request(
        config, method="POST", url=url, session=session, json=payload
    )

    return response.json()


def get_changed_entity_export_job_status(
    config: dict, session: rq.Session, export_job_id: int
) -> dict:
    """
    Gets the status and metadata of a Changed Entity Export Job.

    Args:
        config: Configuration dictionary
        session: Requests session with authentication
        export_job_id: The export job ID

    Returns:
        Job status and metadata dictionary
    """
    url = f"https://api.securevan.com:443/v4/changedEntityExportJobs/{export_job_id}"

    response = make_everyaction_request(config, method="GET", url=url, session=session)

    return response.json()


def wait_for_export_job_completion(
    config: dict, session: rq.Session, export_job_id: int, max_wait_minutes: int = 10
) -> dict:
    """
    Polls the export job status until completion or timeout.

    Args:
        config: Configuration dictionary
        session: Requests session with authentication
        export_job_id: The export job ID
        max_wait_minutes: Maximum time to wait in minutes

    Returns:
        Completed job metadata with download URLs
    """
    import time

    max_wait_seconds = max_wait_minutes * 60
    start_time = time.time()

    while time.time() - start_time < max_wait_seconds:
        job_status = get_changed_entity_export_job_status(
            config, session, export_job_id
        )

        if job_status.get("jobStatus") == "Complete":
            LOG.info("export_complete", export_job_id=export_job_id)
            return job_status
        elif job_status.get("jobStatus") in ["Failed", "Cancelled", "Error"]:
            raise Exception(
                f"Export job {export_job_id} failed: {job_status.get('message')}"
            )

        LOG.debug(
            "export_status",
            export_job_id=export_job_id,
            status=job_status.get("jobStatus"),
        )
        time.sleep(10)  # Wait 10 seconds before checking again

    raise TimeoutError(
        f"Export job {export_job_id} did not complete within {max_wait_minutes} minutes"
    )


def download_and_parse_export_files(
    config: dict, session: rq.Session, job_metadata: dict
) -> Generator[Dict[str, Any], None, None]:
    """
    Downloads and parses CSV files from a completed export job.

    Args:
        config: Configuration dictionary
        session: Requests session with authentication
        job_metadata: Completed job metadata with download URLs

    Yields:
        Parsed records from the CSV files
    """
    files = job_metadata.get("files", [])

    for file_info in files:
        download_url = file_info.get("downloadUrl")
        if not download_url:
            continue

        LOG.info("download_start", url=download_url)

        try:
            # Create a new session without authentication for SAS token URLs
            # The SAS token in the URL provides authentication
            download_session = rq.Session()
            download_session.headers.update({"User-Agent": "Fivetran-Connector/1.0"})

            # Download the CSV file using SAS token authentication
            response = download_session.get(download_url)
            response.raise_for_status()

            # Log the first few lines of the downloaded content for debugging
            content_lines = response.text.split("\n")
            LOG.debug("download_lines", count=len(content_lines))
            if content_lines:
                LOG.debug("download_first_line", preview=content_lines[0][:200])
                if len(content_lines) > 1:
                    LOG.debug("download_second_line", preview=content_lines[1][:200])

            # Parse CSV content directly from response text (memory efficient)
            lines = response.text.splitlines()

            if not lines:
                log.warning("Downloaded file is empty")
                continue

            # Detect delimiter from first line
            first_line = lines[0]
            if "\t" in first_line:
                delimiter = "\t"
                LOG.debug("delimiter_detected", delimiter="tab")
            else:
                delimiter = ","
                LOG.debug("delimiter_detected", delimiter="comma")

            # Create CSV reader from lines (more memory efficient)
            csv_reader = csv.DictReader(lines, delimiter=delimiter)

            # Log the column headers for debugging
            if csv_reader.fieldnames:
                LOG.debug("csv_columns", columns=list(csv_reader.fieldnames))

            # Yield rows one at a time (streaming)
            for row in csv_reader:
                yield row

        except rq.exceptions.HTTPError as e:
            LOG.warning("download_failed", url=download_url, error=str(e))
            # Continue with other files if available
            continue
        except Exception as e:
            LOG.warning("download_unexpected_error", url=download_url, error=str(e))
            continue


def get_changed_entities_incremental(
    config: dict,
    session: rq.Session,
    resource_type: str,
    last_sync_timestamp: str,
    requested_fields: List[str] = None,
) -> Generator[Dict[str, Any], None, None]:
    """
    High-level function to get changed entities since last sync.

    Args:
        config: Configuration dictionary
        session: Requests session with authentication
        resource_type: Type of resource (e.g., "Contacts", "Contributions")
        last_sync_timestamp: ISO datetime string of last sync
        requested_fields: List of specific fields to include (optional)

    Yields:
        Changed entity records
    """
    # Create export job
    job_metadata = create_changed_entity_export_job(
        config,
        session,
        resource_type,
        last_sync_timestamp,
        requested_fields=requested_fields,
    )

    export_job_id = job_metadata.get("exportJobId")
    LOG.info(
        "export_created",
        export_job_id=export_job_id,
        resource_type=resource_type,
        since=last_sync_timestamp,
    )

    # Wait for completion
    completed_job = wait_for_export_job_completion(config, session, export_job_id)

    # Download and parse results
    record_count = 0
    for record in download_and_parse_export_files(config, session, completed_job):
        record_count += 1
        yield record

    LOG.info(
        "export_records_processed", resource_type=resource_type, count=record_count
    )

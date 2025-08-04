from datetime import datetime
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


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
    Incremental sync: Uses Changed Entity Export for efficient updates
    """
    from datetime import datetime
    from everyaction_api import get_everyaction_session

    # Create session with authentication
    session = get_everyaction_session(configuration)

    # Check if this is initial sync or incremental sync
    initial_sync_complete = state.get("initial_sync_complete", False)

    if not initial_sync_complete:
        log.info("Starting initial sync - processing all states")
        yield from perform_initial_sync(configuration, session, state)
    else:
        log.info("Performing incremental sync using Changed Entity Exports")
        yield from perform_incremental_sync(configuration, session, state)


def perform_initial_sync(configuration: dict, session, state: dict):
    """
    Performs initial sync by iterating through all states and fetching all people/contributions.
    Uses state cursor to track progress across sync runs.
    Processes people and contributions page by page for memory efficiency.
    """
    from everyaction_api import get_people_by_state, get_contributions_by_van_id
    from state_codes import US_STATE_CODES
    from datetime import datetime

    # Get current state cursor or start from beginning
    current_state_index = state.get("state_cursor_index", 0)

    # Process states starting from cursor position
    for i in range(current_state_index, len(US_STATE_CODES)):
        state_code = US_STATE_CODES[i]
        log.info(f"Processing state {i+1}/{len(US_STATE_CODES)}: {state_code}")

        # Process people page by page for memory efficiency
        page_count = 0
        total_people_count = 0
        total_contributions_count = 0

        for page_data in get_people_by_state(
            configuration, session, state_code, batch_size=200
        ):
            page_count += 1
            log.info(f"Processing page {page_count} for state {state_code}")

            items = page_data.get("items", [])
            if not items:
                log.info(f"No items in page {page_count} for state {state_code}")
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

            log.info(
                f"Retrieved {len(people_in_page)} people records from page {page_count} for state {state_code}"
            )

            # Upsert people data for this page
            if people_in_page:
                for person in people_in_page:
                    yield op.upsert("people", person)
                total_people_count += len(people_in_page)
                log.info(
                    f"Successfully upserted {len(people_in_page)} people records from page {page_count} of state {state_code}"
                )

            # Get contributions for people in this page
            contributions_in_page = 0
            for van_id in van_ids_in_page:
                for contribution_page_data in get_contributions_by_van_id(
                    configuration, session, van_id, batch_size=200
                ):
                    contribution_items = contribution_page_data.get("items", [])
                    for contribution in contribution_items:
                        contribution = transform_contribution_data(contribution, van_id)
                        yield op.upsert("contributions", contribution)
                        contributions_in_page += 1

            total_contributions_count += contributions_in_page
            log.info(
                f"Successfully upserted {contributions_in_page} contribution records from page {page_count} for state {state_code}"
            )

            # Debug: break after first page for testing
            # break

        log.info(
            f"Completed state {state_code}: {total_people_count} people, {total_contributions_count} contributions"
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
    log.info("Initial sync completed successfully")


def perform_incremental_sync(configuration: dict, session, state: dict):
    """
    Performs incremental sync using Changed Entity Export jobs.
    """
    from everyaction_api import get_changed_entities_incremental
    from datetime import datetime

    last_sync_timestamp = state.get("last_sync_timestamp")
    if not last_sync_timestamp:
        log.info("No last sync timestamp found - falling back to initial sync")
        return perform_initial_sync(configuration, session, {})

    log.info(f"Fetching changes since {last_sync_timestamp}")

    try:
        # Get changed contacts (people)
        contacts_count = 0
        try:
            for contact_record in get_changed_entities_incremental(
                configuration, session, "Contacts", last_sync_timestamp
            ):
                contact = transform_csv_contact_to_person(contact_record)
                if contact:  # Skip records with errors
                    yield op.upsert("people", contact)
                    contacts_count += 1

            log.info(f"Processed {contacts_count} changed contact records")
        except Exception as e:
            log.warning(f"Failed to process contacts: {e}")
            contacts_count = 0

        # Get changed contributions
        contributions_count = 0
        try:
            for contribution_record in get_changed_entities_incremental(
                configuration, session, "Contributions", last_sync_timestamp
            ):
                contribution = transform_csv_contribution_to_contribution(
                    contribution_record
                )
                if contribution:  # Skip records with errors
                    yield op.upsert("contributions", contribution)
                    contributions_count += 1

            log.info(f"Processed {contributions_count} changed contribution records")
        except Exception as e:
            log.warning(f"Failed to process contributions: {e}")
            contributions_count = 0

        if contacts_count == 0 and contributions_count == 0:
            log.warning(
                "No data processed from incremental sync - this might indicate an issue"
            )

    except Exception as e:
        log.warning(f"Incremental sync failed: {e}")
        log.info("Consider falling back to initial sync or investigate the error")
        raise

    # Update last sync timestamp
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
        log.warning(f"Skipping contact with error: {csv_row.get('ErrorMessage')}")
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
        log.warning(f"Skipping contribution with error: {csv_row.get('ErrorMessage')}")
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

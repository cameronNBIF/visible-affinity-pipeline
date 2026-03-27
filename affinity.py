"""
Affinity API Client
===================
Handles all read and write operations against the Affinity CRM API.

Responsibilities:
- Creating an authenticated session for Affinity API calls.
- Looking up Affinity organizations by normalized domain.
- Translating Visible domain-keyed data into Affinity org-ID-keyed data.
- Upserting metric field values on Affinity organization records.
"""

import logging
import os
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from config import METRICS_LAST_UPDATED_FIELD_ID

load_dotenv()

AFFINITY_BASE_URL = os.environ.get("AFFINITY_BASE_URL", "https://api.affinity.co")
AFFINITY_TOKEN = os.environ.get("AFFINITY_ACCESS_TOKEN")

if not AFFINITY_TOKEN:
    raise ValueError(
        "AFFINITY_ACCESS_TOKEN is not set. Please add it to your environment variables."
    )

# Applied to every outbound Affinity request to prevent indefinite hangs.
_REQUEST_TIMEOUT = 30  # seconds


def get_affinity_session() -> requests.Session:
    """
    Creates and returns an authenticated requests.Session for the Affinity API.

    Affinity uses HTTP Basic Auth with an empty username and the API token
    as the password. Callers should create one session and reuse it across
    all operations to avoid repeated authentication overhead.
    """
    session = requests.Session()
    session.auth = ("", AFFINITY_TOKEN)
    return session


def _parse_field_id(field_id_str: str) -> int:
    """
    Extracts the raw integer field ID from a 'field-XXXXX' prefixed string.

    Config stores field IDs in 'field-XXXXX' format to make their origin
    explicit. This helper centralizes the parsing so the format is only
    decoded in one place.

    Args:
        field_id_str: A field ID string in 'field-XXXXX' format.

    Returns:
        The integer portion of the field ID.

    Raises:
        ValueError: If the string is not in the expected format.
    """
    try:
        return int(field_id_str.replace("field-", ""))
    except ValueError:
        raise ValueError(
            f"Invalid field ID format: '{field_id_str}'. Expected 'field-XXXXX'."
        )


def find_organization_by_domain(
    session: requests.Session, domain: str
) -> Optional[int]:
    """
    Searches Affinity for an organization with an exact domain match.

    Uses strict equality on the domain field to prevent false-positive
    substring matches (e.g., 'a.com' incorrectly matching 'ba.com').

    Args:
        session: An authenticated Affinity requests.Session.
        domain:  A normalized domain string (e.g., 'profitual.ai').

    Returns:
        The Affinity organization ID if an exact match is found, otherwise None.
    """
    response = session.get(
        f"{AFFINITY_BASE_URL}/organizations",
        params={"term": domain},
        timeout=_REQUEST_TIMEOUT,
    )

    if not response.ok:
        logging.warning(
            f"  ! Affinity org search failed for '{domain}': HTTP {response.status_code}"
        )
        return None

    for org in response.json().get("organizations", []):
        if org.get("domain", "") == domain:
            return org["id"]

    return None


def get_existing_field_values(
    session: requests.Session, org_id: int
) -> Dict[str, Dict]:
    """
    Fetches all current custom field values for an Affinity organization.

    The returned dict is used for delta comparison before writing, ensuring
    we only make API calls for fields whose values have actually changed.

    Args:
        session: An authenticated Affinity requests.Session.
        org_id:  The Affinity organization ID.

    Returns:
        A dict mapping prefixed field ID strings to their current value metadata:
        { "field-12345": {"value_id": 67890, "value": 50000} }

        Returns an empty dict if the request fails.
    """
    response = session.get(
        f"{AFFINITY_BASE_URL}/field-values",
        params={"organization_id": org_id},
        timeout=_REQUEST_TIMEOUT,
    )

    if not response.ok:
        logging.warning(
            f"  ! Could not fetch existing field values for org {org_id}: "
            f"HTTP {response.status_code}"
        )
        return {}

    existing_fields: Dict[str, Dict] = {}
    for field_value in response.json():
        # Affinity returns field_id as an integer; prefix it to match the
        # 'field-XXXXX' format used as keys in METRIC_MAPPING within config.py.
        field_id_str = f"field-{field_value.get('field_id')}"
        existing_fields[field_id_str] = {
            "value_id": field_value.get("id"),
            "value": field_value.get("value"),
        }

    return existing_fields


def map_visible_to_affinity(
    session: requests.Session, visible_data: Dict[str, Dict]
) -> Dict[int, Dict]:
    """
    Translates a Visible domain-keyed metrics dict into an Affinity org-ID-keyed dict.

    For each domain, searches Affinity for the matching organization. Domains
    with no Affinity match are collected and logged as a summary warning so
    mismatches are easy to action without flooding the log mid-run.

    Args:
        session:      An authenticated Affinity requests.Session.
        visible_data: { "profitual.ai": { "metrics": {...}, "latest_date": "..." } }

    Returns:
        { 12345678: { "metrics": {...}, "latest_date": "..." } }
    """
    logging.info(f"Starting domain matching for {len(visible_data)} companies...")

    mapped_data: Dict[int, Dict] = {}
    missing_domains: List[str] = []

    for domain, data in visible_data.items():
        org_id = find_organization_by_domain(session, domain)

        if org_id:
            mapped_data[org_id] = data
            logging.info(f"  ✓ Matched: {domain} -> Affinity ID {org_id}")
        else:
            missing_domains.append(domain)
            logging.warning(f"  ✗ No match: '{domain}' exists in Visible but not in Affinity.")

    logging.info(
        f"Matching complete. Mapped {len(mapped_data)} / {len(visible_data)} companies."
    )
    if missing_domains:
        logging.warning(
            f"  {len(missing_domains)} unmatched domain(s) skipped: {missing_domains}"
        )

    return mapped_data


def _upsert_field(
    session: requests.Session,
    org_id: int,
    field_id_str: str,
    new_value,
    existing_state: Dict[str, Dict],
) -> Tuple[bool, str]:
    """
    Upserts a single custom field value for an Affinity organization.

    Compares the incoming value against the current Affinity state and routes
    to a PUT (update) or POST (create) accordingly. Returns 'skipped' when
    the value is unchanged to preserve rate-limit budget.

    Args:
        session:        An authenticated Affinity requests.Session.
        org_id:         The Affinity organization ID.
        field_id_str:   The field ID in 'field-XXXXX' format.
        new_value:      The incoming value to write.
        existing_state: The snapshot returned by get_existing_field_values().

    Returns:
        A (success, action) tuple where action is 'updated', 'created', or 'skipped'.
    """
    existing = existing_state.get(field_id_str)
    raw_field_id = _parse_field_id(field_id_str)

    if existing:
        if str(existing["value"]) == str(new_value):
            return True, "skipped"

        response = session.put(
            f"{AFFINITY_BASE_URL}/field-values/{existing['value_id']}",
            json={"value": new_value},
            timeout=_REQUEST_TIMEOUT,
        )
        if response.ok:
            return True, "updated"

        logging.error(
            f"  ✗ FAILED update | Org {org_id} | Field {raw_field_id}: {response.text}"
        )
        return False, "updated"

    response = session.post(
        f"{AFFINITY_BASE_URL}/field-values",
        json={"field_id": raw_field_id, "entity_id": org_id, "value": new_value},
        timeout=_REQUEST_TIMEOUT,
    )
    if response.ok:
        return True, "created"

    logging.error(
        f"  ✗ FAILED create | Org {org_id} | Field {raw_field_id}: {response.text}"
    )
    return False, "created"


def push_to_affinity(
    session: requests.Session, mapped_data: Dict[int, Dict]
) -> None:
    """
    Upserts all metric field values for every matched Affinity organization.

    For each organization, fetches the current Affinity field state and compares
    it against the incoming Visible data. Only changed or missing fields trigger
    a write, preserving rate-limit budget. A summary of all outcomes is logged
    at completion.

    Args:
        session:     An authenticated Affinity requests.Session.
        mapped_data: { org_id: { "metrics": {"field-XXXXX": value}, "latest_date": "YYYY-MM-DD" } }
    """
    logging.info(f"Starting Affinity upsert for {len(mapped_data)} organizations...")

    updated_count = 0
    created_count = 0
    skipped_count = 0
    failed_count = 0

    for org_id, data in mapped_data.items():
        # Work on a shallow copy so we don't mutate the caller's data structure
        # when we inject the 'Metrics Last Updated' timestamp field below.
        metrics = dict(data.get("metrics", {}))
        latest_date = data.get("latest_date")

        if latest_date and latest_date != "0000-00-00":
            metrics[METRICS_LAST_UPDATED_FIELD_ID] = latest_date

        if not metrics:
            logging.debug(f"  — Org {org_id}: no metrics to upsert, skipping.")
            continue

        existing_state = get_existing_field_values(session, org_id)

        for field_id_str, new_value in metrics.items():
            success, action = _upsert_field(
                session, org_id, field_id_str, new_value, existing_state
            )
            raw_field_id = _parse_field_id(field_id_str)

            if action == "updated":
                if success:
                    updated_count += 1
                    logging.info(
                        f"  ✓ UPDATED Org {org_id} | Field {raw_field_id} -> {new_value}"
                    )
                else:
                    failed_count += 1
            elif action == "created":
                if success:
                    created_count += 1
                    logging.info(
                        f"  + CREATED Org {org_id} | Field {raw_field_id} -> {new_value}"
                    )
                else:
                    failed_count += 1
            else:
                skipped_count += 1

    logging.info(
        f"Upsert complete — "
        f"Created: {created_count} | Updated: {updated_count} | "
        f"Skipped (unchanged): {skipped_count} | Failed: {failed_count}"
    )
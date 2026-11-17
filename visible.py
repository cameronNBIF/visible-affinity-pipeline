"""
Visible API Client
==================
Handles all data extraction from the Visible portfolio management platform.

Responsibilities:
- Creating and maintaining an authenticated session with the Visible API.
- Resolving the 'Website' portfolio property ID.
- Fetching the normalized website domain for every portfolio company.
- Extracting the most recent value for each configured metric per company.
- Assembling a master domain-keyed dictionary ready for Affinity ingestion.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from config import METRIC_MAPPING
from utils import normalize_domain

load_dotenv()

def _get_visible_token() -> str:
    token = os.environ.get("VISIBLE_ACCESS_TOKEN")
    if not token:
        raise ValueError("VISIBLE_ACCESS_TOKEN is not set.")
    return token

def _get_visible_company_id() -> str:
    company_id = os.environ.get("VISIBLE_COMPANY_ID")
    if not company_id:
        raise ValueError("VISIBLE_COMPANY_ID is not set.")
    return company_id

VISIBLE_BASE_URL = os.environ.get("VISIBLE_BASE_URL", "https://api.visible.vc")
VISIBLE_TOKEN = _get_visible_token()
VISIBLE_COMPANY_ID = _get_visible_company_id()

# Applied to every outbound Visible request to prevent indefinite hangs.
_REQUEST_TIMEOUT = 30  # seconds

# Retry policy for transient Visible failures. A full run makes several hundred
# requests in quick succession, which is enough to trip Visible's rate limiting
# (HTTP 429). Without retries, every company processed after that point silently
# came back with no metrics and was never updated in Affinity.
#
# Observed live (Nov 2026): Visible sends no Retry-After header, and once rate
# limited it keeps returning 429 for roughly 80-90 seconds. 7 retries with
# capped exponential backoff gives ~3 minutes of total wait (2+4+8+16+32+60+60s),
# enough to ride out a block with margin.
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_MAX_RETRIES = 7
_BACKOFF_BASE_SECONDS = 2
_MAX_BACKOFF_SECONDS = 60


class VisibleAPIError(Exception):
    """Raised when a Visible request still fails after all retries."""


def _retry_delay(response: Optional[requests.Response], attempt: int) -> float:
    """
    Determines how long to wait before retrying a failed request.

    Honours a numeric Retry-After header when Visible provides one; otherwise
    falls back to exponential backoff (2s, 4s, 8s, ...). Always capped.
    """
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), 0.0), _MAX_BACKOFF_SECONDS)
            except ValueError:
                pass  # HTTP-date form; fall back to exponential backoff.
    return min(_BACKOFF_BASE_SECONDS * (2 ** attempt), _MAX_BACKOFF_SECONDS)


def _visible_get(
    session: requests.Session, path: str, params: Dict[str, Any]
) -> Dict:
    """
    Performs a GET against the Visible API with retries, returning parsed JSON.

    Retries rate-limit (429), transient server errors (5xx), timeouts and
    connection errors. Every failure is logged with its status code so it can
    never again disappear silently.

    Raises:
        VisibleAPIError: If the request fails with a non-retryable status, or
                         is still failing after _MAX_RETRIES retries.
    """
    url = f"{VISIBLE_BASE_URL}{path}"

    for attempt in range(_MAX_RETRIES + 1):
        response: Optional[requests.Response] = None
        try:
            response = session.get(url, params=params, timeout=_REQUEST_TIMEOUT)
        except (requests.ConnectionError, requests.Timeout) as exc:
            reason = f"{type(exc).__name__}: {exc}"
        else:
            if response.ok:
                return response.json()
            reason = f"HTTP {response.status_code}"
            if response.status_code not in _RETRYABLE_STATUS_CODES:
                break

        if attempt < _MAX_RETRIES:
            delay = _retry_delay(response, attempt)
            logging.warning(
                f"  ! Visible GET {path} failed ({reason}); "
                f"retry {attempt + 1}/{_MAX_RETRIES} in {delay:.0f}s | params={params}"
            )
            time.sleep(delay)

    body = f" | {response.text[:500]}" if response is not None else ""
    logging.error(f"  ✗ Visible GET {path} failed ({reason}) | params={params}{body}")
    raise VisibleAPIError(f"GET {path} failed ({reason})")


def get_visible_session() -> requests.Session:
    """
    Creates and returns an authenticated requests.Session for the Visible API.

    Callers should create one session and reuse it across all operations to
    take advantage of HTTP keep-alive and avoid repeated handshake overhead.
    """
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {VISIBLE_TOKEN}",
            "Content-Type": "application/json",
        }
    )
    return session


def _fetch_website_property_id(session: requests.Session) -> Optional[str]:
    """
    Fetches the Visible portfolio property ID for the 'Website' field.

    Args:
        session: An authenticated Visible requests.Session.

    Returns:
        The property ID string if found, otherwise None.
    """
    try:
        data = _visible_get(
            session, "/portfolio_properties", {"company_id": VISIBLE_COMPANY_ID}
        )
    except VisibleAPIError:
        logging.error("Failed to fetch portfolio properties.")
        return None

    properties = data.get("portfolio_properties", [])
    website_prop = next(
        (p for p in properties if p["name"].lower().startswith("website")), None
    )

    if not website_prop:
        logging.warning("No 'Website' property found in Visible portfolio properties.")
        return None

    return website_prop["id"]


def _fetch_all_portfolio_companies(session: requests.Session) -> List[Dict]:
    """
    Fetches all portfolio company profiles from Visible, handling pagination.

    Args:
        session: An authenticated Visible requests.Session.

    Returns:
        A flat list of company profile dicts from the Visible API.
    """
    companies: List[Dict] = []
    page = 1

    while True:
        try:
            data = _visible_get(
                session,
                "/portfolio_company_profiles",
                {"company_id": VISIBLE_COMPANY_ID, "page": page},
            )
        except VisibleAPIError:
            logging.error(
                f"Failed to fetch portfolio companies (page {page}); "
                f"continuing with {len(companies)} companies collected so far."
            )
            break

        companies.extend(data.get("portfolio_company_profiles", []))

        if page >= data.get("meta", {}).get("total_pages", 1):
            break
        page += 1

    return companies


def _fetch_company_website(
    session: requests.Session, profile_id: str, website_property_id: str
) -> Optional[str]:
    """
    Fetches and normalizes the website domain for a single portfolio company.

    Args:
        session:             An authenticated Visible requests.Session.
        profile_id:          The Visible portfolio company profile ID.
        website_property_id: The Visible property ID for the 'Website' field.

    Returns:
        A normalized domain string (e.g., 'profitual.ai'), or None if the
        website is missing, empty, or explicitly set to 'N/A'.

    Raises:
        VisibleAPIError: If the property values could not be fetched. This is
                         kept distinct from "no website" so the caller can
                         report the company as failed rather than skip it.
    """
    data = _visible_get(
        session,
        "/portfolio_property_values",
        {"portfolio_company_profile_id": profile_id},
    )

    values = data.get("portfolio_property_values", [])
    for value in values:
        if value.get("portfolio_property_id") == website_property_id:
            raw_url = value.get("value")
            if raw_url and raw_url != "N/A":
                return normalize_domain(raw_url) or None
            # Found the property but it has no usable value — no need to check further.
            return None

    return None


def fetch_all_company_websites(
    session: requests.Session, failures: Optional[List[str]] = None
) -> Dict[str, str]:
    """
    Builds a mapping of Visible profile IDs to normalized website domains.

    Resolves the 'Website' portfolio property ID, fetches all portfolio company
    profiles, then retrieves and normalizes the website for each company.

    Args:
        session:  An authenticated Visible requests.Session.
        failures: Optional list; a description of every company whose website
                  could not be fetched is appended to it.

    Returns:
        { "profile_id": "profitual.ai", ... } for all companies with a valid website.
        Returns an empty dict if the 'Website' property cannot be resolved.
    """
    logging.info("Fetching 'Website' property ID from Visible...")
    website_property_id = _fetch_website_property_id(session)
    if not website_property_id:
        return {}

    logging.info("Fetching all portfolio company profiles...")
    companies = _fetch_all_portfolio_companies(session)
    logging.info(f"Mapping website domains for {len(companies)} companies...")

    domain_map: Dict[str, str] = {}
    for company in companies:
        profile_id = str(company["id"])
        try:
            domain = _fetch_company_website(session, profile_id, website_property_id)
        except VisibleAPIError as exc:
            name = company.get("name", profile_id)
            logging.error(f"  ✗ Could not fetch website for '{name}' ({profile_id}).")
            if failures is not None:
                failures.append(f"{name} (profile {profile_id}): website lookup - {exc}")
            continue
        if domain:
            domain_map[profile_id] = domain

    return domain_map


def get_latest_metric_data_point(
    session: requests.Session, metric_id: str
) -> Tuple[Optional[float], str]:
    """
    Fetches the most recent valid data point for a specific Visible metric.

    Iterates all pages of historical data points for the given metric and
    returns the single most recent value-and-date pair found.

    Args:
        session:   An authenticated Visible requests.Session.
        metric_id: The Visible metric ID to query.

    Returns:
        A (value, date) tuple where value is a float and date is 'YYYY-MM-DD'.
        Returns (None, '0000-00-00') if no valid data points are found.

    Raises:
        VisibleAPIError: If any page fails. A partial scan could return an
                         older value as the "latest", so nothing is returned.
    """
    latest_value: Optional[float] = None
    latest_date = "0000-00-00"
    page = 1

    while True:
        data = _visible_get(
            session,
            "/data_points",
            {"metric_id": metric_id, "page": page, "page_size": 100},
        )

        for point in data.get("data_points", []):
            point_date = point.get("date")
            point_value = point.get("value")

            if point_value not in (None, "None") and point_date and point_date > latest_date:
                latest_date = point_date
                latest_value = float(point_value)

        meta = data.get("meta", {})
        total_pages = int(meta.get("total_pages") or meta.get("pages") or 1)
        if page >= total_pages:
            break
        page += 1

    return latest_value, latest_date


def extract_company_metrics(
    session: requests.Session,
    profile_id: str,
    domain: str,
    target_metric_names: Dict[str, str],
) -> Dict:
    """
    Extracts all configured metric values for a single portfolio company.

    Fetches every metric available for the company in Visible (with pagination),
    filters against the configured metric names, then fetches the most recent
    data point for each match. Values are stored keyed by their Affinity field ID.

    Args:
        session:             An authenticated Visible requests.Session.
        profile_id:          The Visible portfolio company profile ID.
        domain:              The company's normalized domain (used for log clarity).
        target_metric_names: Lowercase metric name → canonical config name lookup.

    Returns:
        {
            "metrics":     { "field-XXXXX": float_value, ... },
            "latest_date": "YYYY-MM-DD"   (newest date across all extracted metrics),
            "errors":      [ "description", ... ]  (empty when extraction was complete)
        }
        Metrics that could not be fetched are left out of "metrics" so their
        Affinity values are not touched, and are listed in "errors".
    """
    company_data: Dict = {"metrics": {}, "latest_date": "0000-00-00", "errors": []}
    all_metrics: List[Dict] = []
    page = 1

    while True:
        try:
            data = _visible_get(
                session,
                "/metrics",
                {
                    "company_id": VISIBLE_COMPANY_ID,
                    "filter[portfolio_company_profile_id]": profile_id,
                    "page": page,
                },
            )
        except VisibleAPIError as exc:
            company_data["errors"].append(f"metric list (page {page}) - {exc}")
            break

        all_metrics.extend(data.get("metrics", []))

        meta = data.get("meta", {})
        total_pages = int(meta.get("total_pages") or meta.get("pages") or 1)
        if page >= total_pages:
            break
        page += 1

    for metric in all_metrics:
        metric_name_lower = metric.get("name", "").strip().lower()

        if metric_name_lower not in target_metric_names:
            continue

        canonical_name = target_metric_names[metric_name_lower]
        affinity_field_id = METRIC_MAPPING[canonical_name]["affinity_field_id"]

        try:
            latest_value, latest_date = get_latest_metric_data_point(
                session, metric["id"]
            )
        except VisibleAPIError as exc:
            company_data["errors"].append(f"{canonical_name} - {exc}")
            logging.error(f"  ✗ [{domain}] {canonical_name}: could not fetch data points.")
            continue

        if latest_value is not None:
            company_data["metrics"][affinity_field_id] = latest_value
            logging.info(
                f"  ✓ [{domain}] {canonical_name}: {latest_value} (as of {latest_date})"
            )

            if latest_date > company_data["latest_date"]:
                company_data["latest_date"] = latest_date

    return company_data


def get_bulk_portfolio_metrics(failures: Optional[List[str]] = None) -> Dict[str, Dict]:
    """
    Orchestrates the full Visible extraction flow for all portfolio companies.

    Fetches the website domain for every portfolio company, then extracts all
    configured metrics for each company. The result is keyed by normalized
    domain for downstream Affinity matching.

    Args:
        failures: Optional list; a description of every company whose Visible
                  data could not be fully extracted is appended to it, so the
                  caller can fail the run after pushing whatever did succeed.

    Returns:
        {
            "profitual.ai": {
                "metrics":     { "field-XXXXX": value, ... },
                "latest_date": "YYYY-MM-DD"
            },
            ...
        }
        Returns an empty dict if no companies with valid websites are found.
    """
    if failures is None:
        failures = []

    session = get_visible_session()
    domain_map = fetch_all_company_websites(session, failures)

    if not domain_map:
        logging.warning("No companies with valid websites found in Visible.")
        return {}

    # Build a lowercase lookup so metric name matching is case-insensitive.
    # Visible metric names sometimes have inconsistent casing or trailing spaces.
    target_metric_names: Dict[str, str] = {
        name.lower().strip(): name for name in METRIC_MAPPING.keys()
    }

    logging.info(f"Fetching metrics for {len(domain_map)} companies...")
    master_data: Dict[str, Dict] = {}

    for profile_id, domain in domain_map.items():
        company_data = extract_company_metrics(
            session, profile_id, domain, target_metric_names
        )
        master_data[domain] = company_data
        failures.extend(f"{domain}: {err}" for err in company_data["errors"])

    with_metrics = sum(1 for d in master_data.values() if d["metrics"])
    logging.info(
        f"Extraction complete. {with_metrics} / {len(master_data)} companies "
        f"returned metrics."
    )
    if failures:
        logging.error(
            f"Visible extraction incomplete — {len(failures)} failure(s): {failures}"
        )
    return master_data
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
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from config import METRIC_MAPPING
from utils import normalize_domain

load_dotenv()

VISIBLE_BASE_URL = os.environ.get("VISIBLE_BASE_URL", "https://api.visible.vc")
VISIBLE_TOKEN = os.environ.get("VISIBLE_ACCESS_TOKEN")
VISIBLE_COMPANY_ID = os.environ.get("VISIBLE_COMPANY_ID")

if not VISIBLE_TOKEN:
    raise ValueError(
        "VISIBLE_ACCESS_TOKEN is not set. Please add it to your environment variables."
    )
if not VISIBLE_COMPANY_ID:
    raise ValueError(
        "VISIBLE_COMPANY_ID is not set. Please add it to your environment variables."
    )

# Applied to every outbound Visible request to prevent indefinite hangs.
_REQUEST_TIMEOUT = 30  # seconds


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
    response = session.get(
        f"{VISIBLE_BASE_URL}/portfolio_properties",
        params={"company_id": VISIBLE_COMPANY_ID},
        timeout=_REQUEST_TIMEOUT,
    )

    if not response.ok:
        logging.error(
            f"Failed to fetch portfolio properties: "
            f"HTTP {response.status_code} | {response.text}"
        )
        return None

    properties = response.json().get("portfolio_properties", [])
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
        response = session.get(
            f"{VISIBLE_BASE_URL}/portfolio_company_profiles",
            params={"company_id": VISIBLE_COMPANY_ID, "page": page},
            timeout=_REQUEST_TIMEOUT,
        )

        if not response.ok:
            logging.error(
                f"Failed to fetch portfolio companies (page {page}): "
                f"HTTP {response.status_code}"
            )
            break

        data = response.json()
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
    """
    response = session.get(
        f"{VISIBLE_BASE_URL}/portfolio_property_values",
        params={"portfolio_company_profile_id": profile_id},
        timeout=_REQUEST_TIMEOUT,
    )

    if not response.ok:
        return None

    values = response.json().get("portfolio_property_values", [])
    for value in values:
        if value.get("portfolio_property_id") == website_property_id:
            raw_url = value.get("value")
            if raw_url and raw_url != "N/A":
                return normalize_domain(raw_url) or None
            # Found the property but it has no usable value — no need to check further.
            return None

    return None


def fetch_all_company_websites(session: requests.Session) -> Dict[str, str]:
    """
    Builds a mapping of Visible profile IDs to normalized website domains.

    Resolves the 'Website' portfolio property ID, fetches all portfolio company
    profiles, then retrieves and normalizes the website for each company.

    Args:
        session: An authenticated Visible requests.Session.

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
        domain = _fetch_company_website(session, profile_id, website_property_id)
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
    """
    latest_value: Optional[float] = None
    latest_date = "0000-00-00"
    page = 1

    while True:
        response = session.get(
            f"{VISIBLE_BASE_URL}/data_points",
            params={"metric_id": metric_id, "page": page, "page_size": 100},
            timeout=_REQUEST_TIMEOUT,
        )

        if not response.ok:
            break

        data = response.json()
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
            "latest_date": "YYYY-MM-DD"   (newest date across all extracted metrics)
        }
    """
    company_data: Dict = {"metrics": {}, "latest_date": "0000-00-00"}
    all_metrics: List[Dict] = []
    page = 1

    while True:
        response = session.get(
            f"{VISIBLE_BASE_URL}/metrics",
            params={
                "company_id": VISIBLE_COMPANY_ID,
                "filter[portfolio_company_profile_id]": profile_id,
                "page": page,
            },
            timeout=_REQUEST_TIMEOUT,
        )

        if not response.ok:
            break

        data = response.json()
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

        latest_value, latest_date = get_latest_metric_data_point(session, metric["id"])

        if latest_value is not None:
            company_data["metrics"][affinity_field_id] = latest_value
            logging.info(
                f"  ✓ [{domain}] {canonical_name}: {latest_value} (as of {latest_date})"
            )

            if latest_date > company_data["latest_date"]:
                company_data["latest_date"] = latest_date

    return company_data


def get_bulk_portfolio_metrics() -> Dict[str, Dict]:
    """
    Orchestrates the full Visible extraction flow for all portfolio companies.

    Fetches the website domain for every portfolio company, then extracts all
    configured metrics for each company. The result is keyed by normalized
    domain for downstream Affinity matching.

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
    session = get_visible_session()
    domain_map = fetch_all_company_websites(session)

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
        master_data[domain] = extract_company_metrics(
            session, profile_id, domain, target_metric_names
        )

    logging.info(f"Extraction complete. Retrieved data for {len(master_data)} companies.")
    return master_data
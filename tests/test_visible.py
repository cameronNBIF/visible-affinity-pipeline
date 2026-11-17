"""
Tests for visible.py
====================
Covers the full Visible API client: authentication, pagination, domain
resolution, metric extraction, and the top-level orchestration function.

Testing strategy
----------------
- All HTTP traffic is intercepted by the `responses` library — no real network
  calls are made, and any accidental outbound request will raise a
  ConnectionError that fails the test immediately.
- Environment variables are patched via monkeypatch / module-level fixtures so
  tests are hermetic and do not depend on a local .env file.
- Helper fixtures (build_company, build_data_point, …) produce realistic API
  payloads that mirror the actual Visible API shape, making failures easy to
  interpret and the test data easy to extend.
- Each test validates one specific behaviour; the test name reads like a
  sentence describing exactly what is being asserted.
"""

import os
import pytest
import responses as rsps_lib
from unittest.mock import patch, MagicMock

# ---------------------------------------------------------------------------
# Environment bootstrap
# ---------------------------------------------------------------------------
# visible.py reads env vars at *import* time, so we must patch os.environ
# before the module is first imported in any test session.
# ---------------------------------------------------------------------------

os.environ["VISIBLE_ACCESS_TOKEN"] = "test_token"
os.environ["VISIBLE_COMPANY_ID"] = "test_company_id"
os.environ.setdefault("VISIBLE_BASE_URL", "https://api.visible.vc")

import visible  # noqa: E402 – must come after env bootstrap
from visible import (
    get_visible_session,
    _fetch_website_property_id,
    _fetch_all_portfolio_companies,
    _fetch_company_website,
    fetch_all_company_websites,
    get_latest_metric_data_point,
    extract_company_metrics,
    get_bulk_portfolio_metrics,
    VisibleAPIError,
    _visible_get,
    _MAX_RETRIES,
)

# ---------------------------------------------------------------------------
# Constants reused across tests
# ---------------------------------------------------------------------------

BASE_URL = "https://api.visible.vc"
COMPANY_ID = "test_company_id"
PROFILES_URL = f"{BASE_URL}/portfolio_company_profiles"
PROPERTIES_URL = f"{BASE_URL}/portfolio_properties"
PROPERTY_VALUES_URL = f"{BASE_URL}/portfolio_property_values"
DATA_POINTS_URL = f"{BASE_URL}/data_points"
METRICS_URL = f"{BASE_URL}/metrics"

WEBSITE_PROP_ID = "prop-website-1"
PROFILE_ID = "profile-42"
ORG_DOMAIN = "profitual.ai"
METRIC_ID = "metric-99"


# ---------------------------------------------------------------------------
# Payload helpers — mirror real Visible API shapes
# ---------------------------------------------------------------------------

def build_company(profile_id: str, name: str = "Test Co") -> dict:
    return {"id": profile_id, "name": name}


def build_meta(total_pages: int, current_page: int = 1) -> dict:
    return {"total_pages": total_pages, "page": current_page}


def build_property(prop_id: str, name: str) -> dict:
    return {"id": prop_id, "name": name}


def build_property_value(prop_id: str, value: str) -> dict:
    return {"portfolio_property_id": prop_id, "value": value}


def build_data_point(value, date: str) -> dict:
    return {"value": value, "date": date}


def build_metric(metric_id: str, name: str) -> dict:
    return {"id": metric_id, "name": name}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def no_sleep():
    """Retry backoff must never actually sleep during tests; records requested delays."""
    with patch.object(visible.time, "sleep") as mock_sleep:
        yield mock_sleep


@pytest.fixture
def session():
    """A live authenticated Visible session (no real HTTP calls needed)."""
    return get_visible_session()


@pytest.fixture
def target_metric_names():
    """Lowercase metric-name lookup as produced inside get_bulk_portfolio_metrics."""
    from config import METRIC_MAPPING
    return {name.lower().strip(): name for name in METRIC_MAPPING.keys()}


# ===========================================================================
# get_visible_session
# ===========================================================================

class TestGetVisibleSession:

    def test_returns_requests_session(self, session):
        import requests
        assert isinstance(session, requests.Session)

    def test_authorization_header_uses_bearer_token(self, session):
        assert session.headers["Authorization"] == "Bearer test_token"

    def test_content_type_is_json(self, session):
        assert session.headers["Content-Type"] == "application/json"


# ===========================================================================
# _fetch_website_property_id
# ===========================================================================

class TestFetchWebsitePropertyId:

    @rsps_lib.activate
    def test_returns_id_when_website_property_exists(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "Website")]},
            status=200,
        )
        result = _fetch_website_property_id(session)
        assert result == WEBSITE_PROP_ID

    @rsps_lib.activate
    def test_match_is_case_insensitive_for_website_prefix(self, session):
        """Property names like 'website URL' or 'WEBSITE' should still match."""
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "WEBSITE URL")]},
            status=200,
        )
        result = _fetch_website_property_id(session)
        assert result == WEBSITE_PROP_ID

    @rsps_lib.activate
    def test_returns_none_when_no_website_property(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property("prop-2", "Sector")]},
            status=200,
        )
        result = _fetch_website_property_id(session)
        assert result is None

    @rsps_lib.activate
    def test_returns_none_when_properties_list_is_empty(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": []},
            status=200,
        )
        result = _fetch_website_property_id(session)
        assert result is None

    @rsps_lib.activate
    def test_returns_none_on_http_error(self, session):
        rsps_lib.add(rsps_lib.GET, PROPERTIES_URL, status=500)
        result = _fetch_website_property_id(session)
        assert result is None

    @rsps_lib.activate
    def test_returns_none_on_unauthorized(self, session):
        rsps_lib.add(rsps_lib.GET, PROPERTIES_URL, status=401)
        result = _fetch_website_property_id(session)
        assert result is None

    @rsps_lib.activate
    def test_selects_first_website_property_when_multiple_present(self, session):
        """When multiple 'website*' properties exist, the first match wins."""
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={
                "portfolio_properties": [
                    build_property("prop-a", "Website"),
                    build_property("prop-b", "Website (Secondary)"),
                ]
            },
            status=200,
        )
        result = _fetch_website_property_id(session)
        assert result == "prop-a"


# ===========================================================================
# _fetch_all_portfolio_companies
# ===========================================================================

class TestFetchAllPortfolioCompanies:

    @rsps_lib.activate
    def test_single_page_returns_all_companies(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company("c1"), build_company("c2")],
                "meta": build_meta(total_pages=1),
            },
            status=200,
        )
        result = _fetch_all_portfolio_companies(session)
        assert len(result) == 2

    @rsps_lib.activate
    def test_pagination_aggregates_companies_across_all_pages(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company("c1")],
                "meta": build_meta(total_pages=3),
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company("c2")],
                "meta": build_meta(total_pages=3),
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company("c3")],
                "meta": build_meta(total_pages=3),
            },
            status=200,
        )
        result = _fetch_all_portfolio_companies(session)
        assert len(result) == 3
        assert [c["id"] for c in result] == ["c1", "c2", "c3"]

    @rsps_lib.activate
    def test_returns_empty_list_on_http_error(self, session):
        rsps_lib.add(rsps_lib.GET, PROFILES_URL, status=503)
        result = _fetch_all_portfolio_companies(session)
        assert result == []

    @rsps_lib.activate
    def test_returns_empty_list_when_no_companies(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={"portfolio_company_profiles": [], "meta": build_meta(total_pages=1)},
            status=200,
        )
        result = _fetch_all_portfolio_companies(session)
        assert result == []

    @rsps_lib.activate
    def test_stops_paginating_after_error_on_subsequent_page(self, session):
        """If page 2 fails, returns whatever was collected on page 1."""
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company("c1")],
                "meta": build_meta(total_pages=2),
            },
            status=200,
        )
        rsps_lib.add(rsps_lib.GET, PROFILES_URL, status=500)
        result = _fetch_all_portfolio_companies(session)
        assert len(result) == 1
        assert result[0]["id"] == "c1"

    @rsps_lib.activate
    def test_missing_meta_defaults_to_single_page(self, session):
        """A response with no 'meta' key should be treated as a single page."""
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={"portfolio_company_profiles": [build_company("c1")]},
            status=200,
        )
        result = _fetch_all_portfolio_companies(session)
        assert len(result) == 1


# ===========================================================================
# _fetch_company_website
# ===========================================================================

class TestFetchCompanyWebsite:

    @rsps_lib.activate
    def test_returns_normalized_domain_for_valid_url(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "https://www.profitual.ai/home")
            ]},
            status=200,
        )
        result = _fetch_company_website(session, PROFILE_ID, WEBSITE_PROP_ID)
        assert result == "profitual.ai"

    @rsps_lib.activate
    def test_returns_none_when_website_value_is_na(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "N/A")
            ]},
            status=200,
        )
        result = _fetch_company_website(session, PROFILE_ID, WEBSITE_PROP_ID)
        assert result is None

    @rsps_lib.activate
    def test_returns_none_when_website_value_is_empty_string(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "")
            ]},
            status=200,
        )
        result = _fetch_company_website(session, PROFILE_ID, WEBSITE_PROP_ID)
        assert result is None

    @rsps_lib.activate
    def test_returns_none_when_property_not_found_in_values(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value("prop-other", "https://other.com")
            ]},
            status=200,
        )
        result = _fetch_company_website(session, PROFILE_ID, WEBSITE_PROP_ID)
        assert result is None

    @rsps_lib.activate
    def test_raises_on_http_error(self, session):
        """An API failure must be distinguishable from 'company has no website'."""
        rsps_lib.add(rsps_lib.GET, PROPERTY_VALUES_URL, status=404)
        with pytest.raises(VisibleAPIError):
            _fetch_company_website(session, PROFILE_ID, WEBSITE_PROP_ID)

    @rsps_lib.activate
    def test_returns_none_when_normalize_domain_yields_empty(self, session):
        """A value like 'N/A' or a non-URL that normalizes to '' should return None."""
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "not-a-valid-domain")
            ]},
            status=200,
        )
        # normalize_domain("not-a-valid-domain") → "" because there's no dot
        result = _fetch_company_website(session, PROFILE_ID, WEBSITE_PROP_ID)
        assert result is None

    @rsps_lib.activate
    def test_strips_www_prefix_from_website(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "https://www.example.com")
            ]},
            status=200,
        )
        result = _fetch_company_website(session, PROFILE_ID, WEBSITE_PROP_ID)
        assert result == "example.com"

    @rsps_lib.activate
    def test_early_exit_after_finding_website_property(self, session):
        """Once the matching property is found, no further values should be inspected."""
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "https://first.com"),
                # A second entry for the same property ID should never exist, but
                # the function must return the first match only.
                build_property_value(WEBSITE_PROP_ID, "https://second.com"),
            ]},
            status=200,
        )
        result = _fetch_company_website(session, PROFILE_ID, WEBSITE_PROP_ID)
        assert result == "first.com"


# ===========================================================================
# fetch_all_company_websites
# ===========================================================================

class TestFetchAllCompanyWebsites:

    @rsps_lib.activate
    def test_returns_domain_map_for_companies_with_valid_websites(self, session):
        # Properties endpoint
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "Website")]},
            status=200,
        )
        # Profiles endpoint (single page)
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company(PROFILE_ID, "Profitual")],
                "meta": build_meta(total_pages=1),
            },
            status=200,
        )
        # Property values for the one company
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "https://profitual.ai")
            ]},
            status=200,
        )
        result = fetch_all_company_websites(session)
        assert result == {PROFILE_ID: "profitual.ai"}

    @rsps_lib.activate
    def test_excludes_companies_with_no_website(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "Website")]},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company(PROFILE_ID)],
                "meta": build_meta(total_pages=1),
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "N/A")
            ]},
            status=200,
        )
        result = fetch_all_company_websites(session)
        assert result == {}

    @rsps_lib.activate
    def test_returns_empty_dict_when_website_property_not_found(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": []},
            status=200,
        )
        result = fetch_all_company_websites(session)
        assert result == {}

    @rsps_lib.activate
    def test_handles_multiple_companies_correctly(self, session):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "Website")]},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [
                    build_company("p1"),
                    build_company("p2"),
                ],
                "meta": build_meta(total_pages=1),
            },
            status=200,
        )
        # Website for p1
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "https://alpha.com")
            ]},
            status=200,
        )
        # No website for p2
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": []},
            status=200,
        )
        result = fetch_all_company_websites(session)
        assert result == {"p1": "alpha.com"}


# ===========================================================================
# get_latest_metric_data_point
# ===========================================================================

class TestGetLatestMetricDataPoint:

    @rsps_lib.activate
    def test_returns_most_recent_value_from_single_page(self, session):
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [
                    build_data_point(100, "2024-01-01"),
                    build_data_point(200, "2024-06-01"),
                    build_data_point(150, "2024-03-01"),
                ],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        value, date = get_latest_metric_data_point(session, METRIC_ID)
        assert value == 200.0
        assert date == "2024-06-01"

    @rsps_lib.activate
    def test_returns_most_recent_value_across_multiple_pages(self, session):
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(100, "2024-01-01")],
                "meta": {"total_pages": 2},
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(999, "2025-01-01")],
                "meta": {"total_pages": 2},
            },
            status=200,
        )
        value, date = get_latest_metric_data_point(session, METRIC_ID)
        assert value == 999.0
        assert date == "2025-01-01"

    @rsps_lib.activate
    def test_returns_none_and_sentinel_date_when_no_data_points(self, session):
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={"data_points": [], "meta": {"total_pages": 1}},
            status=200,
        )
        value, date = get_latest_metric_data_point(session, METRIC_ID)
        assert value is None
        assert date == "0000-00-00"

    @rsps_lib.activate
    def test_raises_on_persistent_http_error(self, session):
        """A failure must not be mistaken for 'metric has no data'."""
        rsps_lib.add(rsps_lib.GET, DATA_POINTS_URL, status=500)
        with pytest.raises(VisibleAPIError):
            get_latest_metric_data_point(session, METRIC_ID)

    @rsps_lib.activate
    def test_raises_when_later_page_fails_instead_of_returning_partial_latest(
        self, session
    ):
        """A partial scan could report an older value as the latest, so it must fail."""
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(10, "2024-01-01")],
                "meta": {"total_pages": 2},
            },
            status=200,
        )
        rsps_lib.add(rsps_lib.GET, DATA_POINTS_URL, status=429)
        with pytest.raises(VisibleAPIError):
            get_latest_metric_data_point(session, METRIC_ID)

    @rsps_lib.activate
    def test_skips_data_points_with_none_value(self, session):
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [
                    build_data_point(None, "2024-06-01"),
                    build_data_point(50, "2024-01-01"),
                ],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        value, date = get_latest_metric_data_point(session, METRIC_ID)
        # The None-valued point has a later date but must be skipped
        assert value == 50.0
        assert date == "2024-01-01"

    @rsps_lib.activate
    def test_skips_data_points_with_string_none_value(self, session):
        """Visible sometimes serialises missing values as the string 'None'."""
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [
                    build_data_point("None", "2024-09-01"),
                    build_data_point(75, "2024-03-01"),
                ],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        value, date = get_latest_metric_data_point(session, METRIC_ID)
        assert value == 75.0
        assert date == "2024-03-01"

    @rsps_lib.activate
    def test_handles_meta_using_pages_key_instead_of_total_pages(self, session):
        """Some Visible endpoints use 'pages' rather than 'total_pages' in meta."""
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(42, "2024-07-01")],
                "meta": {"pages": 1},
            },
            status=200,
        )
        value, date = get_latest_metric_data_point(session, METRIC_ID)
        assert value == 42.0

    @rsps_lib.activate
    def test_coerces_string_numeric_value_to_float(self, session):
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point("123.45", "2024-05-01")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        value, date = get_latest_metric_data_point(session, METRIC_ID)
        assert value == 123.45
        assert isinstance(value, float)


# ===========================================================================
# extract_company_metrics
# ===========================================================================

class TestExtractCompanyMetrics:

    @rsps_lib.activate
    def test_extracts_configured_metric_and_stores_by_affinity_field_id(
        self, session, target_metric_names
    ):
        from config import METRIC_MAPPING

        canonical = "Full-time Employees"
        affinity_field_id = METRIC_MAPPING[canonical]["affinity_field_id"]
        metric_name_in_visible = canonical  # exact match

        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={
                "metrics": [build_metric(METRIC_ID, metric_name_in_visible)],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(25, "2024-09-30")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )

        result = extract_company_metrics(
            session, PROFILE_ID, ORG_DOMAIN, target_metric_names
        )

        assert affinity_field_id in result["metrics"]
        assert result["metrics"][affinity_field_id] == 25.0
        assert result["latest_date"] == "2024-09-30"

    @rsps_lib.activate
    def test_ignores_metrics_not_in_config(self, session, target_metric_names):
        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={
                "metrics": [build_metric(METRIC_ID, "Unknown Metric XYZ")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        result = extract_company_metrics(
            session, PROFILE_ID, ORG_DOMAIN, target_metric_names
        )
        assert result["metrics"] == {}

    @rsps_lib.activate
    def test_returns_empty_metrics_when_no_metrics_exist_for_company(
        self, session, target_metric_names
    ):
        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={"metrics": [], "meta": {"total_pages": 1}},
            status=200,
        )
        result = extract_company_metrics(
            session, PROFILE_ID, ORG_DOMAIN, target_metric_names
        )
        assert result == {"metrics": {}, "latest_date": "0000-00-00", "errors": []}

    @rsps_lib.activate
    def test_latest_date_reflects_most_recent_across_multiple_metrics(
        self, session, target_metric_names
    ):
        from config import METRIC_MAPPING

        m1_name = "Full-time Employees"
        m1_id = "metric-1"
        m1_field = METRIC_MAPPING[m1_name]["affinity_field_id"]

        m2_name = "Cash Position"
        m2_id = "metric-2"
        m2_field = METRIC_MAPPING[m2_name]["affinity_field_id"]

        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={
                "metrics": [
                    build_metric(m1_id, m1_name),
                    build_metric(m2_id, m2_name),
                ],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        # Older data point for FTE
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(10, "2024-03-01")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        # Newer data point for Cash Position
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(500000, "2024-11-01")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )

        result = extract_company_metrics(
            session, PROFILE_ID, ORG_DOMAIN, target_metric_names
        )
        assert result["latest_date"] == "2024-11-01"
        assert m1_field in result["metrics"]
        assert m2_field in result["metrics"]

    @rsps_lib.activate
    def test_metric_name_matching_is_case_insensitive(
        self, session, target_metric_names
    ):
        from config import METRIC_MAPPING

        # Visible sometimes returns mixed-case metric names
        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={
                "metrics": [build_metric(METRIC_ID, "FULL-TIME EMPLOYEES")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(8, "2024-08-01")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )

        result = extract_company_metrics(
            session, PROFILE_ID, ORG_DOMAIN, target_metric_names
        )
        expected_field = METRIC_MAPPING["Full-time Employees"]["affinity_field_id"]
        assert expected_field in result["metrics"]

    @rsps_lib.activate
    def test_skips_metric_when_data_point_value_is_none(
        self, session, target_metric_names
    ):
        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={
                "metrics": [build_metric(METRIC_ID, "Cash Position")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={"data_points": [], "meta": {"total_pages": 1}},
            status=200,
        )

        result = extract_company_metrics(
            session, PROFILE_ID, ORG_DOMAIN, target_metric_names
        )
        assert result["metrics"] == {}
        assert result["latest_date"] == "0000-00-00"

    @rsps_lib.activate
    def test_paginates_through_metrics_endpoint(self, session, target_metric_names):
        from config import METRIC_MAPPING

        m1_name = "Full-time Employees"
        m2_name = "Cash Position"

        # Metrics spread over two pages
        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={
                "metrics": [build_metric("m1", m1_name)],
                "meta": {"total_pages": 2},
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={
                "metrics": [build_metric("m2", m2_name)],
                "meta": {"total_pages": 2},
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(5, "2024-01-01")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(100000, "2024-02-01")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )

        result = extract_company_metrics(
            session, PROFILE_ID, ORG_DOMAIN, target_metric_names
        )
        assert len(result["metrics"]) == 2


# ===========================================================================
# get_bulk_portfolio_metrics (orchestration)
# ===========================================================================

class TestGetBulkPortfolioMetrics:

    @rsps_lib.activate
    def test_returns_domain_keyed_metrics_for_all_companies(self):
        from config import METRIC_MAPPING

        canonical = "Full-time Employees"
        affinity_field_id = METRIC_MAPPING[canonical]["affinity_field_id"]

        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "Website")]},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company(PROFILE_ID)],
                "meta": build_meta(total_pages=1),
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "https://profitual.ai")
            ]},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={
                "metrics": [build_metric(METRIC_ID, canonical)],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={
                "data_points": [build_data_point(12, "2024-10-01")],
                "meta": {"total_pages": 1},
            },
            status=200,
        )

        result = get_bulk_portfolio_metrics()

        assert "profitual.ai" in result
        assert affinity_field_id in result["profitual.ai"]["metrics"]
        assert result["profitual.ai"]["metrics"][affinity_field_id] == 12.0
        assert result["profitual.ai"]["latest_date"] == "2024-10-01"

    @rsps_lib.activate
    def test_returns_empty_dict_when_no_companies_have_websites(self):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "Website")]},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company(PROFILE_ID)],
                "meta": build_meta(total_pages=1),
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": []},
            status=200,
        )

        result = get_bulk_portfolio_metrics()
        assert result == {}

    @rsps_lib.activate
    def test_returns_empty_dict_when_website_property_is_missing(self):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": []},
            status=200,
        )
        result = get_bulk_portfolio_metrics()
        assert result == {}

    @rsps_lib.activate
    def test_company_with_no_matching_metrics_still_appears_in_result(self):
        """A company with a valid website but zero matching metrics returns an empty metrics dict."""
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "Website")]},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company(PROFILE_ID)],
                "meta": build_meta(total_pages=1),
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "https://empty.io")
            ]},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={"metrics": [], "meta": {"total_pages": 1}},
            status=200,
        )

        result = get_bulk_portfolio_metrics()
        assert "empty.io" in result
        assert result["empty.io"]["metrics"] == {}

# ===========================================================================
# _visible_get - retry / backoff behaviour
# ===========================================================================

class TestVisibleGet:

    @rsps_lib.activate
    def test_returns_json_on_success_without_sleeping(self, session, no_sleep):
        rsps_lib.add(rsps_lib.GET, METRICS_URL, json={"ok": True}, status=200)
        assert _visible_get(session, "/metrics", {}) == {"ok": True}
        no_sleep.assert_not_called()

    @rsps_lib.activate
    def test_retries_rate_limit_then_succeeds(self, session, no_sleep):
        rsps_lib.add(rsps_lib.GET, METRICS_URL, status=429)
        rsps_lib.add(rsps_lib.GET, METRICS_URL, json={"ok": True}, status=200)
        assert _visible_get(session, "/metrics", {}) == {"ok": True}
        assert no_sleep.call_count == 1
        assert len(rsps_lib.calls) == 2

    @rsps_lib.activate
    def test_honours_numeric_retry_after_header(self, session, no_sleep):
        rsps_lib.add(rsps_lib.GET, METRICS_URL, status=429, headers={"Retry-After": "7"})
        rsps_lib.add(rsps_lib.GET, METRICS_URL, json={}, status=200)
        _visible_get(session, "/metrics", {})
        no_sleep.assert_called_once_with(7.0)

    @rsps_lib.activate
    def test_retry_after_is_capped(self, session, no_sleep):
        rsps_lib.add(rsps_lib.GET, METRICS_URL, status=429, headers={"Retry-After": "3600"})
        rsps_lib.add(rsps_lib.GET, METRICS_URL, json={}, status=200)
        _visible_get(session, "/metrics", {})
        assert no_sleep.call_args[0][0] == visible._MAX_BACKOFF_SECONDS

    @rsps_lib.activate
    def test_uses_exponential_backoff_without_retry_after(self, session, no_sleep):
        for _ in range(3):
            rsps_lib.add(rsps_lib.GET, METRICS_URL, status=503)
        rsps_lib.add(rsps_lib.GET, METRICS_URL, json={}, status=200)
        _visible_get(session, "/metrics", {})
        assert [c.args[0] for c in no_sleep.call_args_list] == [2, 4, 8]

    @rsps_lib.activate
    def test_raises_after_exhausting_retries(self, session, no_sleep):
        rsps_lib.add(rsps_lib.GET, METRICS_URL, status=429)
        with pytest.raises(VisibleAPIError, match="HTTP 429"):
            _visible_get(session, "/metrics", {})
        assert len(rsps_lib.calls) == _MAX_RETRIES + 1
        assert no_sleep.call_count == _MAX_RETRIES

    @rsps_lib.activate
    def test_does_not_retry_client_errors(self, session, no_sleep):
        rsps_lib.add(rsps_lib.GET, METRICS_URL, status=401)
        with pytest.raises(VisibleAPIError, match="HTTP 401"):
            _visible_get(session, "/metrics", {})
        assert len(rsps_lib.calls) == 1
        no_sleep.assert_not_called()

    @rsps_lib.activate
    def test_retries_connection_errors(self, session, no_sleep):
        import requests
        rsps_lib.add(rsps_lib.GET, METRICS_URL, body=requests.ConnectionError("reset"))
        rsps_lib.add(rsps_lib.GET, METRICS_URL, json={"ok": True}, status=200)
        assert _visible_get(session, "/metrics", {}) == {"ok": True}

    @rsps_lib.activate
    def test_logs_status_code_on_final_failure(self, session, caplog):
        rsps_lib.add(rsps_lib.GET, METRICS_URL, status=429)
        with pytest.raises(VisibleAPIError):
            _visible_get(session, "/metrics", {"page": 1})
        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert errors and "HTTP 429" in errors[-1].getMessage()


# ===========================================================================
# Failure reporting - companies must never silently come back empty
# ===========================================================================

class TestExtractionFailureReporting:

    @rsps_lib.activate
    def test_rate_limited_company_is_still_extracted_after_retry(
        self, session, target_metric_names
    ):
        """Regression: rate limiting used to make every later company come back empty."""
        from config import METRIC_MAPPING
        field_id = METRIC_MAPPING["Cash Position"]["affinity_field_id"]

        rsps_lib.add(rsps_lib.GET, METRICS_URL, status=429)
        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={"metrics": [build_metric(METRIC_ID, "Cash Position")], "meta": {"total_pages": 1}},
            status=200,
        )
        rsps_lib.add(rsps_lib.GET, DATA_POINTS_URL, status=429)
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={"data_points": [build_data_point(2002536, "2026-07-01")], "meta": {"total_pages": 1}},
            status=200,
        )

        result = extract_company_metrics(session, PROFILE_ID, ORG_DOMAIN, target_metric_names)

        assert result["metrics"] == {field_id: 2002536.0}
        assert result["latest_date"] == "2026-07-01"
        assert result["errors"] == []

    @rsps_lib.activate
    def test_metric_list_failure_is_recorded_as_error(self, session, target_metric_names):
        rsps_lib.add(rsps_lib.GET, METRICS_URL, status=429)
        result = extract_company_metrics(session, PROFILE_ID, ORG_DOMAIN, target_metric_names)
        assert result["metrics"] == {}
        assert len(result["errors"]) == 1
        assert "HTTP 429" in result["errors"][0]

    @rsps_lib.activate
    def test_failed_metric_is_omitted_but_others_are_kept(self, session, target_metric_names):
        from config import METRIC_MAPPING
        from responses import matchers

        cash_field = METRIC_MAPPING["Cash Position"]["affinity_field_id"]
        fte_field = METRIC_MAPPING["Full-time Employees"]["affinity_field_id"]

        rsps_lib.add(
            rsps_lib.GET, METRICS_URL,
            json={
                "metrics": [
                    build_metric("m-cash", "Cash Position"),
                    build_metric("m-fte", "Full-time Employees"),
                ],
                "meta": {"total_pages": 1},
            },
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            json={"data_points": [build_data_point(500, "2026-07-01")], "meta": {"total_pages": 1}},
            status=200,
            match=[matchers.query_param_matcher({"metric_id": "m-cash"}, strict_match=False)],
        )
        rsps_lib.add(
            rsps_lib.GET, DATA_POINTS_URL,
            status=500,
            match=[matchers.query_param_matcher({"metric_id": "m-fte"}, strict_match=False)],
        )

        result = extract_company_metrics(session, PROFILE_ID, ORG_DOMAIN, target_metric_names)

        assert result["metrics"] == {cash_field: 500.0}
        assert fte_field not in result["metrics"]
        assert len(result["errors"]) == 1
        assert result["errors"][0].startswith("Full-time Employees")

    @rsps_lib.activate
    def test_bulk_reports_per_company_failures(self):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "Website")]},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={"portfolio_company_profiles": [build_company(PROFILE_ID)], "meta": build_meta(1)},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROPERTY_VALUES_URL,
            json={"portfolio_property_values": [
                build_property_value(WEBSITE_PROP_ID, "https://beauceronsecurity.com")
            ]},
            status=200,
        )
        rsps_lib.add(rsps_lib.GET, METRICS_URL, status=429)

        failures = []
        result = get_bulk_portfolio_metrics(failures)

        assert result["beauceronsecurity.com"]["metrics"] == {}
        assert len(failures) == 1
        assert failures[0].startswith("beauceronsecurity.com:")

    @rsps_lib.activate
    def test_bulk_reports_website_lookup_failures(self):
        rsps_lib.add(
            rsps_lib.GET, PROPERTIES_URL,
            json={"portfolio_properties": [build_property(WEBSITE_PROP_ID, "Website")]},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.GET, PROFILES_URL,
            json={
                "portfolio_company_profiles": [build_company(PROFILE_ID, "Beauceron")],
                "meta": build_meta(1),
            },
            status=200,
        )
        rsps_lib.add(rsps_lib.GET, PROPERTY_VALUES_URL, status=503)

        failures = []
        result = get_bulk_portfolio_metrics(failures)

        assert result == {}
        assert len(failures) == 1
        assert "Beauceron" in failures[0]

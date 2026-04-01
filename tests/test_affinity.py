"""
Tests for affinity.py
=====================
Covers the full Affinity API client: session authentication, field ID parsing,
organization domain resolution, existing field retrieval, and the delta-sync 
upsert engine.

Testing strategy
----------------
- All HTTP traffic is intercepted by the `responses` library.
- Environment variables are patched before import.
- Each function is tested for both the "happy path" and HTTP error states to 
  ensure the pipeline degrades gracefully without crashing.
- Upsert logic is rigorously tested to ensure unchanged values are skipped, 
  saving API rate limits.
"""

import os
import pytest
import responses as rsps_lib

# ---------------------------------------------------------------------------
# Environment bootstrap
# ---------------------------------------------------------------------------
os.environ["AFFINITY_ACCESS_TOKEN"] = "test_affinity_token"
os.environ.setdefault("AFFINITY_BASE_URL", "https://api.affinity.co")

import affinity  # noqa: E402
from affinity import (
    get_affinity_session,
    _parse_field_id,
    find_organization_by_domain,
    get_existing_field_values,
    map_visible_to_affinity,
    _upsert_field,
    push_to_affinity,
)
from config import METRICS_LAST_UPDATED_FIELD_ID

# ---------------------------------------------------------------------------
# Constants reused across tests
# ---------------------------------------------------------------------------
BASE_URL = "https://api.affinity.co"
ORGS_URL = f"{BASE_URL}/organizations"
FIELD_VALUES_URL = f"{BASE_URL}/field-values"

TEST_DOMAIN = "profitual.ai"
TEST_ORG_ID = 123456


# ---------------------------------------------------------------------------
# Payload helpers — mirror real Affinity API shapes
# ---------------------------------------------------------------------------
def build_org(org_id: int, domain: str) -> dict:
    return {"id": org_id, "domain": domain}

def build_field_value(value_id: int, field_id: int, value: any) -> dict:
    return {"id": value_id, "field_id": field_id, "value": value}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def session():
    """A live authenticated Affinity session."""
    return get_affinity_session()


# ===========================================================================
# get_affinity_session
# ===========================================================================
class TestGetAffinitySession:
    def test_returns_requests_session(self, session):
        import requests
        assert isinstance(session, requests.Session)

    def test_auth_uses_http_basic_with_empty_username(self, session):
        # Affinity requires HTTP Basic Auth: username is empty, password is the token
        assert session.auth == ("", "test_affinity_token")


# ===========================================================================
# _parse_field_id
# ===========================================================================
class TestParseFieldId:
    def test_successfully_extracts_integer_from_valid_format(self):
        assert _parse_field_id("field-5626428") == 5626428

    def test_raises_value_error_on_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid field ID format"):
            _parse_field_id("invalid-format")
            
    def test_raises_value_error_on_non_numeric_id(self):
        with pytest.raises(ValueError):
            _parse_field_id("field-ABC")


# ===========================================================================
# find_organization_by_domain
# ===========================================================================
class TestFindOrganizationByDomain:
    @rsps_lib.activate
    def test_returns_org_id_on_exact_match(self, session):
        rsps_lib.add(
            rsps_lib.GET, ORGS_URL,
            json={"organizations": [build_org(TEST_ORG_ID, TEST_DOMAIN)]},
            status=200
        )
        assert find_organization_by_domain(session, TEST_DOMAIN) == TEST_ORG_ID

    @rsps_lib.activate
    def test_ignores_fuzzy_substring_matches(self, session):
        """If searching for 'profitual.ai', it should NOT match 'notprofitual.ai'."""
        rsps_lib.add(
            rsps_lib.GET, ORGS_URL,
            json={"organizations": [build_org(999, "notprofitual.ai")]},
            status=200
        )
        assert find_organization_by_domain(session, TEST_DOMAIN) is None

    @rsps_lib.activate
    def test_returns_none_when_no_organizations_returned(self, session):
        rsps_lib.add(rsps_lib.GET, ORGS_URL, json={"organizations": []}, status=200)
        assert find_organization_by_domain(session, TEST_DOMAIN) is None

    @rsps_lib.activate
    def test_returns_none_on_http_error(self, session):
        rsps_lib.add(rsps_lib.GET, ORGS_URL, status=500)
        assert find_organization_by_domain(session, TEST_DOMAIN) is None


# ===========================================================================
# get_existing_field_values
# ===========================================================================
class TestGetExistingFieldValues:
    @rsps_lib.activate
    def test_returns_mapped_dict_of_existing_fields(self, session):
        rsps_lib.add(
            rsps_lib.GET, FIELD_VALUES_URL,
            json=[
                build_field_value(value_id=111, field_id=5626428, value=50000),
                build_field_value(value_id=222, field_id=5626485, value=12000)
            ],
            status=200
        )
        result = get_existing_field_values(session, TEST_ORG_ID)
        
        assert "field-5626428" in result
        assert result["field-5626428"]["value_id"] == 111
        assert result["field-5626428"]["value"] == 50000
        
        assert "field-5626485" in result
        assert result["field-5626485"]["value"] == 12000

    @rsps_lib.activate
    def test_returns_empty_dict_on_http_error(self, session):
        rsps_lib.add(rsps_lib.GET, FIELD_VALUES_URL, status=404)
        result = get_existing_field_values(session, TEST_ORG_ID)
        assert result == {}


# ===========================================================================
# map_visible_to_affinity
# ===========================================================================
class TestMapVisibleToAffinity:
    @rsps_lib.activate
    def test_successfully_maps_domains_to_org_ids(self, session):
        rsps_lib.add(
            rsps_lib.GET, ORGS_URL,
            json={"organizations": [build_org(TEST_ORG_ID, TEST_DOMAIN)]},
            status=200
        )
        
        visible_data = {
            TEST_DOMAIN: {"metrics": {"field-123": 10}, "latest_date": "2024-01-01"}
        }
        
        result = map_visible_to_affinity(session, visible_data)
        
        assert TEST_ORG_ID in result
        assert result[TEST_ORG_ID]["latest_date"] == "2024-01-01"
        assert TEST_DOMAIN not in result

    @rsps_lib.activate
    def test_skips_domains_not_found_in_affinity(self, session):
        rsps_lib.add(rsps_lib.GET, ORGS_URL, json={"organizations": []}, status=200)
        
        visible_data = {
            "missing.com": {"metrics": {"field-123": 10}, "latest_date": "2024-01-01"}
        }
        
        result = map_visible_to_affinity(session, visible_data)
        assert result == {}


# ===========================================================================
# _upsert_field
# ===========================================================================
class TestUpsertField:
    def test_skips_when_value_is_identical(self, session):
        existing_state = {"field-123": {"value_id": 999, "value": 50000.0}}
        success, action = _upsert_field(session, TEST_ORG_ID, "field-123", 50000.0, existing_state)
        
        assert success is True
        assert action == "skipped"

    def test_skips_when_value_is_identical_but_types_differ(self, session):
        """Should skip if existing is string '50000.0' and new is float 50000.0"""
        existing_state = {"field-123": {"value_id": 999, "value": "50000.0"}}
        success, action = _upsert_field(session, TEST_ORG_ID, "field-123", 50000.0, existing_state)
        
        assert success is True
        assert action == "skipped"

    @rsps_lib.activate
    def test_updates_existing_field_when_value_changes(self, session):
        existing_state = {"field-123": {"value_id": 999, "value": 10.0}}
        
        rsps_lib.add(rsps_lib.PUT, f"{FIELD_VALUES_URL}/999", status=200)
        
        success, action = _upsert_field(session, TEST_ORG_ID, "field-123", 20.0, existing_state)
        
        assert success is True
        assert action == "updated"

    @rsps_lib.activate
    def test_creates_new_field_when_no_existing_state(self, session):
        existing_state = {} # Field doesn't exist yet
        
        rsps_lib.add(rsps_lib.POST, FIELD_VALUES_URL, status=200)
        
        success, action = _upsert_field(session, TEST_ORG_ID, "field-123", 20.0, existing_state)
        
        assert success is True
        assert action == "created"

    @rsps_lib.activate
    def test_handles_put_failure(self, session):
        existing_state = {"field-123": {"value_id": 999, "value": 10.0}}
        rsps_lib.add(rsps_lib.PUT, f"{FIELD_VALUES_URL}/999", status=500)
        
        success, action = _upsert_field(session, TEST_ORG_ID, "field-123", 20.0, existing_state)
        assert success is False
        assert action == "updated"

    @rsps_lib.activate
    def test_handles_post_failure(self, session):
        existing_state = {}
        rsps_lib.add(rsps_lib.POST, FIELD_VALUES_URL, status=403)
        
        success, action = _upsert_field(session, TEST_ORG_ID, "field-123", 20.0, existing_state)
        assert success is False
        assert action == "created"


# ===========================================================================
# push_to_affinity
# ===========================================================================
class TestPushToAffinity:
    @rsps_lib.activate
    def test_injects_metrics_last_updated_date(self, session):
        """Ensures the orchestrator correctly pushes the 'latest_date' as a metric."""
        rsps_lib.add(rsps_lib.GET, FIELD_VALUES_URL, json=[], status=200)
        rsps_lib.add(rsps_lib.POST, FIELD_VALUES_URL, status=200)

        mapped_data = {
            TEST_ORG_ID: {
                "metrics": {"field-123": 50}, 
                "latest_date": "2024-05-01"
            }
        }
        
        push_to_affinity(session, mapped_data)
        
        # We expect 2 POSTs: one for field-123, one for METRICS_LAST_UPDATED_FIELD_ID
        assert len(rsps_lib.calls) == 3 # 1 GET, 2 POSTs
        
        # Verify the date was injected into the payload
        post_payloads = [call.request.body for call in rsps_lib.calls if call.request.method == "POST"]
        assert any(f'"field_id": {METRICS_LAST_UPDATED_FIELD_ID.replace("field-", "")}' in str(p) for p in post_payloads)
        assert any('"value": "2024-05-01"' in str(p) for p in post_payloads)

    @rsps_lib.activate
    def test_skips_org_if_metrics_are_empty(self, session):
        mapped_data = {
            TEST_ORG_ID: {"metrics": {}, "latest_date": "0000-00-00"}
        }
        
        push_to_affinity(session, mapped_data)
        
        # Zero HTTP calls should be made if there are no metrics
        assert len(rsps_lib.calls) == 0

    @rsps_lib.activate
    def test_does_not_mutate_original_dict(self, session):
        """Ensures the injection of the date doesn't alter the caller's dictionary permanently."""
        rsps_lib.add(rsps_lib.GET, FIELD_VALUES_URL, json=[], status=200)
        rsps_lib.add(rsps_lib.POST, FIELD_VALUES_URL, status=200)

        mapped_data = {
            TEST_ORG_ID: {"metrics": {"field-123": 50}, "latest_date": "2024-05-01"}
        }
        
        push_to_affinity(session, mapped_data)
        
        # The METRICS_LAST_UPDATED_FIELD_ID should NOT be permanently in mapped_data
        assert METRICS_LAST_UPDATED_FIELD_ID not in mapped_data[TEST_ORG_ID]["metrics"]
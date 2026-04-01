# tests/conftest.py
import pytest
import os

@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Automatically mock environment variables for every test.
    This prevents tests from accidentally hitting production credentials.
    """
    monkeypatch.setenv("AFFINITY_ACCESS_TOKEN", "test_affinity_token")
    monkeypatch.setenv("VISIBLE_ACCESS_TOKEN", "test_visible_token")
    monkeypatch.setenv("VISIBLE_COMPANY_ID", "test_company_id")
    monkeypatch.setenv("AFFINITY_BASE_URL", "https://api.affinity.co")
    monkeypatch.setenv("VISIBLE_BASE_URL", "https://api.visible.vc")
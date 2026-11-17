"""
Tests for main.py
=================
Covers run-level failure reporting: Visible extraction failures must not be
hidden behind a successful-looking run.
"""

import os
from unittest.mock import patch

import pytest

# Assign (not setdefault) so a developer's local .env, loaded by an earlier
# import of affinity.py, can never leak real credentials into the test modules.
os.environ["VISIBLE_ACCESS_TOKEN"] = "test_token"
os.environ["VISIBLE_COMPANY_ID"] = "test_company_id"
os.environ["AFFINITY_ACCESS_TOKEN"] = "test_affinity_token"

import main  # noqa: E402 - must come after env bootstrap

VISIBLE_DATA = {
    "climative.ai": {"metrics": {"field-1": 1.0}, "latest_date": "2026-07-01", "errors": []}
}
AFFINITY_DATA = {307396423: VISIBLE_DATA["climative.ai"]}


def _run_main(failures_to_report):
    """Runs main() with all external phases mocked; returns the push mock."""

    def fake_extract(failures):
        failures.extend(failures_to_report)
        return VISIBLE_DATA

    with patch.object(main, "get_bulk_portfolio_metrics", side_effect=fake_extract), \
         patch.object(main, "get_affinity_session"), \
         patch.object(main, "map_visible_to_affinity", return_value=AFFINITY_DATA), \
         patch.object(main, "push_to_affinity") as mock_push:
        try:
            main.main()
        finally:
            _run_main.push = mock_push


def test_clean_run_does_not_raise():
    _run_main([])
    _run_main.push.assert_called_once()


def test_extraction_failures_still_push_then_raise():
    with pytest.raises(RuntimeError, match="beauceronsecurity.com"):
        _run_main(
            ["beauceronsecurity.com: metric list (page 1) - GET /metrics failed (HTTP 429)"]
        )
    # Companies that were extracted successfully are still pushed before the run fails.
    _run_main.push.assert_called_once()

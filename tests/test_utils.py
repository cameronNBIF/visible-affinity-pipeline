# tests/test_utils.py
import pytest
from utils import normalize_domain

@pytest.mark.parametrize("input_url, expected_output", [
    ("https://www.profitual.ai", "profitual.ai"),
    ("http://troj.ai/home", "troj.ai"),
    ("www.google.com", "google.com"),
    ("  https://SPACEY.com/  ", "spacey.com"),
    ("localhost:8080", "localhost"), # Edge case testing
    (None, ""),                      # Type safety testing
    ("", ""),
])
def test_normalize_domain(input_url, expected_output):
    assert normalize_domain(input_url) == expected_output
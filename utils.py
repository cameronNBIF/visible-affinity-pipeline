import re
from urllib.parse import urlparse

def normalize_domain(url: str) -> str:
    """
    Standardizes a URL into a clean domain name for matching.
    Example: 'https://www.google.com/search' -> 'google.com'
    """
    if not url or not isinstance(url, str):
        return ""

    # 1. Lowercase and strip whitespace
    url = url.strip().lower()

    # 2. Add a scheme if missing so urlparse works correctly
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    try:
        # 3. Parse the URL
        parsed = urlparse(url)
        domain = parsed.netloc

        # 4. Remove 'www.' prefix if it exists
        if domain.startswith('www.'):
            domain = domain[4:]

        # 5. Remove any port numbers (e.g., localhost:8080)
        domain = domain.split(':')[0]

        # 6. Basic validation: ensure there's at least one dot
        if '.' not in domain:
            return ""

        return domain
    except Exception:
        return ""
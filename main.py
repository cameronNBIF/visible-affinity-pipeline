"""
Visible → Affinity Metrics Sync Pipeline
=========================================
Extracts the most recent portfolio company metrics from the Visible platform
and upserts them into the corresponding Affinity organization records.

Pipeline phases:
  Phase 1 — Extract metrics for all portfolio companies from Visible.
  Phase 2 — Translate Visible domains to Affinity organization IDs.
  Phase 3 — Upsert the delta into Affinity (only changed or missing fields).

Usage:
    python main.py
"""

import logging
import sys

from affinity import get_affinity_session, map_visible_to_affinity, push_to_affinity
from visible import get_bulk_portfolio_metrics

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def main() -> None:
    """
    Runs the three-phase Visible → Affinity sync pipeline.

    Exits early with a warning if any phase produces no usable data,
    avoiding unnecessary downstream API calls.
    """
    # Phase 1: Extract metrics from Visible
    logging.info("=" * 20)
    logging.info("Phase 1: Extracting portfolio metrics from Visible...")
    logging.info("=" * 20)
    visible_data = get_bulk_portfolio_metrics()

    if not visible_data:
        logging.warning("No portfolio data returned from Visible. Aborting pipeline.")
        sys.exit(0)

    # Phase 2: Translate Visible domains to Affinity organization IDs.
    # A single session is created here and shared across both remaining phases
    # to avoid redundant authentication overhead.
    logging.info("=" * 20)
    logging.info("Phase 2: Matching Visible domains to Affinity organizations...")
    logging.info("=" * 20)
    affinity_session = get_affinity_session()
    affinity_data = map_visible_to_affinity(affinity_session, visible_data)

    if not affinity_data:
        logging.warning("No Affinity organizations were matched. Aborting pipeline.")
        sys.exit(0)

    # Phase 3: Upsert the metric delta into Affinity
    logging.info("=" * 20)
    logging.info("Phase 3: Pushing metric updates to Affinity...")
    logging.info("=" * 20)
    push_to_affinity(affinity_session, affinity_data)


if __name__ == "__main__":
    main()
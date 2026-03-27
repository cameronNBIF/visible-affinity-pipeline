import logging
from visible import get_bulk_portfolio_metrics
from affinity import map_visible_to_affinity, get_affinity_session
from affinity import push_to_affinity

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    # Phase 2: Extract from Visible
    visible_raw_data = get_bulk_portfolio_metrics()
    
    # Phase 3: Translate Domains to Affinity Org IDs
    affinity_mapped_data = map_visible_to_affinity(visible_raw_data)
    
    # Phase 4: Push the Delta Sync to Affinity
    affinity_session = get_affinity_session()
    push_to_affinity(affinity_session, affinity_mapped_data)

if __name__ == "__main__":
    main()
import os
import requests
import logging
from typing import Dict
from config import METRICS_LAST_UPDATED_FIELD_ID
from dotenv import load_dotenv

load_dotenv()

AFFINITY_BASE_URL = os.environ.get("AFFINITY_BASE_URL", "https://api.affinity.co")
AFFINITY_TOKEN = os.environ.get("AFFINITY_ACCESS_TOKEN")

def get_affinity_session() -> requests.Session:
    """Creates a persistent session for Affinity API calls."""
    session = requests.Session()
    # Affinity uses HTTP Basic Auth with the API token as the password
    session.auth = ('', AFFINITY_TOKEN) 
    return session

def find_organization_by_domain(session: requests.Session, domain: str) -> int:
    """
    Searches Affinity for an organization matching the exact domain.
    Returns the Organization ID if found, or None if missing.
    """
    params = {"term": domain}
    r = session.get(f"{AFFINITY_BASE_URL}/organizations", params=params)
    
    if r.ok:
        results = r.json().get("organizations", [])
        
        # We strictly verify the domain to prevent fuzzy-matching the wrong company
        for org in results:
            if domain in org.get("domain", ""):
                return org["id"]
                
    return None

def map_visible_to_affinity(visible_data: Dict[str, Dict]) -> Dict[int, Dict]:
    """
    Translates the Visible master dictionary into an Affinity-ready dictionary.
    Input:  { "profitual.ai": { "metrics": {...}, "latest_date": "..." } }
    Output: { 12345678: { "metrics": {...}, "latest_date": "..." } }
    """
    logging.info(f"Starting Affinity matching for {len(visible_data)} domains...")
    session = get_affinity_session()
    
    affinity_mapped_data = {}
    missing_companies = []
    
    for domain, data in visible_data.items():
        org_id = find_organization_by_domain(session, domain)
        
        if org_id:
            affinity_mapped_data[org_id] = data
            logging.info(f"  ✓ Matched: {domain} -> Affinity ID {org_id}")
        else:
            missing_companies.append(domain)
            logging.warning(f"  X Missing: '{domain}' exists in Visible but not in Affinity.")
            
    logging.info(f"Matching complete. Successfully mapped {len(affinity_mapped_data)} companies.")
    if missing_companies:
        logging.warning(f"Skipped {len(missing_companies)} companies due to missing Affinity records.")
        
    return affinity_mapped_data

def get_existing_field_values(session: requests.Session, org_id: int) -> Dict[str, Dict]:
    """
    Fetches all existing custom field values for an organization.
    Returns: { "field_id": {"value_id": 12345, "value": 50000} }
    """
    r = session.get(
        f"{AFFINITY_BASE_URL}/field-values", 
        params={"organization_id": org_id}
    )
    
    existing_fields = {}
    if r.ok:
        for fv in r.json():
            # Affinity returns field_id as an integer, we cast to string to match our config 'field-XXXXX' format
            field_id_str = f"field-{fv.get('field_id')}" 
            existing_fields[field_id_str] = {
                "value_id": fv.get("id"),
                "value": fv.get("value")
            }
    return existing_fields

def push_to_affinity(session: requests.Session, mapped_data: Dict[int, Dict]):
    """
    The Master Upsert Engine.
    Input: { 12345678: { "metrics": {"field-123": 500}, "latest_date": "2025-10-01" } }
    """
    logging.info(f"Starting Affinity Upsert for {len(mapped_data)} organizations...")
    
    updated_count = 0
    skipped_count = 0

    for org_id, data in mapped_data.items():
        metrics = data.get("metrics", {})
        latest_date = data.get("latest_date")
        
        # Add the 'Metrics Last Updated' date to our metrics payload so we process it in the same loop
        if latest_date and latest_date != "0000-00-00":
            metrics[METRICS_LAST_UPDATED_FIELD_ID] = latest_date
            
        if not metrics:
            continue # Nothing to update for this company
            
        # 1. Get current Affinity state
        existing_state = get_existing_field_values(session, org_id)
        
        # 2. Compare and Upsert
        for field_id_str, new_val in metrics.items():
            # Extract the raw integer ID for the Affinity API payload
            raw_field_id = int(field_id_str.replace("field-", "")) 
            
            existing = existing_state.get(field_id_str)
            
            # SCENARIO A: The field exists, but the value has changed
            if existing:
                if str(existing["value"]) != str(new_val):
                    value_id = existing["value_id"]
                    r = session.put(
                        f"{AFFINITY_BASE_URL}/field-values/{value_id}",
                        json={"value": new_val}
                    )
                    if r.ok:
                        updated_count += 1
                        logging.info(f"  ✓ UPDATED Org {org_id} | Field {raw_field_id} -> {new_val}")
                    else:
                        logging.error(f"  X FAILED Update Org {org_id}: {r.text}")
                else:
                    skipped_count += 1
                    # Values are identical, skip to save rate limits
                    
            # SCENARIO B: The field is completely empty in Affinity
            else:
                r = session.post(
                    f"{AFFINITY_BASE_URL}/field-values",
                    json={
                        "field_id": raw_field_id,
                        "entity_id": org_id,
                        "value": new_val
                    }
                )
                if r.ok:
                    updated_count += 1
                    logging.info(f"  + CREATED Org {org_id} | Field {raw_field_id} -> {new_val}")
                else:
                    logging.error(f"  X FAILED Create Org {org_id}: {r.text}")

    logging.info(f"Upsert Complete! Made {updated_count} updates. Skipped {skipped_count} unchanged fields.")
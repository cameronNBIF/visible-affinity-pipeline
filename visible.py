import os
import requests
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from config import METRIC_MAPPING
from utils import normalize_domain
import logging

load_dotenv()

VISIBLE_BASE_URL = os.environ.get("VISIBLE_BASE_URL", "https://api.visible.vc")
VISIBLE_TOKEN = os.environ.get("VISIBLE_ACCESS_TOKEN")
VISIBLE_COMPANY_ID = os.environ.get("VISIBLE_COMPANY_ID")

def get_visible_session() -> requests.Session:
    """Creates a persistent session to speed up bulk API calls."""
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {VISIBLE_TOKEN}",
        "Content-Type": "application/json"
    })
    return session

def fetch_all_company_websites(session: requests.Session) -> Dict[str, str]:
    """
    Fetches the 'Website' property for companies.
    Uses a highly reliable profile loop to bypass API filter limitations.
    """
    logging.info("Fetching Website property ID...")
    response = session.get(f"{VISIBLE_BASE_URL}/portfolio_properties", params={"company_id": VISIBLE_COMPANY_ID})
    
    if not response.ok:
        logging.error(f"Visible API Error! Status: {response.status_code} | Body: {response.text}")
        return {}

    properties = response.json().get("portfolio_properties", [])
    website_prop_id = next((p["id"] for p in properties if p["name"].lower().startswith("website")), None)
    
    if not website_prop_id:
        logging.warning("No 'Website' property found in Visible!")
        return {}

    logging.info("Fetching portfolio companies to map websites...")
    websites = {}
    
    # 1. Get the master list of all company profiles
    companies = []
    page = 1
    while True:
        response = session.get(
            f"{VISIBLE_BASE_URL}/portfolio_company_profiles", 
            params={"company_id": VISIBLE_COMPANY_ID, "page": page}
        )
        if not response.ok: 
            break
            
        data = response.json()
        companies.extend(data.get("portfolio_company_profiles", []))
        
        if page >= data.get("meta", {}).get("total_pages", 1): 
            break
        page += 1

    # 2. Safely grab the website property for each company
    for comp in companies:
        comp_id = str(comp["id"])
        
        response = session.get(
            f"{VISIBLE_BASE_URL}/portfolio_property_values", 
            params={"portfolio_company_profile_id": comp_id} # Asking for just this company
        )
        
        if response.ok:
            values = response.json().get("portfolio_property_values", [])
            for v in values:
                if v.get("portfolio_property_id") == website_prop_id:
                    raw_url = v.get("value")
                    if raw_url and raw_url != "N/A":
                        normalized = normalize_domain(raw_url)
                        if normalized:
                            websites[comp_id] = normalized
                    break # Found the website, stop checking other properties for this company

    return websites

def get_latest_metric_data_point(session: requests.Session, metric_id: str) -> Tuple[Optional[float], str]:
    """
    Responsibility: Fetches the historical data points for a specific metric 
    and returns the most recent valid value and its date.
    """
    dp_req = session.get(
        f"{VISIBLE_BASE_URL}/data_points",
        params={"metric_id": metric_id, "page_size": 100} 
    )
    
    if not dp_req.ok:
        return None, "0000-00-00"
        
    points = dp_req.json().get("data_points", [])
    latest_val = None
    latest_date = "0000-00-00"
    
    for dp in points:
        dp_date = dp.get("date")
        dp_val = dp.get("value")
        
        # Check if it's a valid number and newer than our current latest_date
        if dp_val not in [None, "None"] and dp_date and dp_date > latest_date:
            latest_date = dp_date
            latest_val = float(dp_val)
            
    return latest_val, latest_date

def extract_company_metrics(session: requests.Session, profile_id: str, domain: str, target_metric_names: Dict[str, str], print_debug: bool = False) -> Dict:
    """
    Responsibility: Fetches a company's available metrics (handling pagination), 
    filters them against our config, and maps the values directly to their Affinity Field IDs.
    """
    company_data = {"metrics": {}, "latest_date": "0000-00-00"}
    company_metrics = []
    page = 1
    
    # NEW: Pagination loop to fetch all metric pages
    while True:
        r = session.get(
            f"{VISIBLE_BASE_URL}/metrics", 
            params={
                "company_id": VISIBLE_COMPANY_ID, 
                "filter[portfolio_company_profile_id]": profile_id,
                "page": page
            }
        )
        if not r.ok: 
            break
            
        data = r.json()
        company_metrics.extend(data.get("metrics", []))
        
        meta = data.get("meta", {})
        total_pages = int(meta.get("total_pages") or meta.get("pages") or 1)
        if page >= total_pages:
            break
        page += 1
    
    # Debug tool to see exactly what Visible calls their metrics (Runs once)
    if print_debug and company_metrics:
        logging.info("\n--- ACTUAL METRIC NAMES FOUND IN VISIBLE ---")
        for m in company_metrics:
            logging.info(f"Visible Name: '{m.get('name', '')}'")
        logging.info("--------------------------------------------\n")
        
    for metric in company_metrics:
        metric_name_lower = metric.get("name", "").strip().lower()
        
        # Check if this metric exists in config.py
        if metric_name_lower in target_metric_names:
            visible_config_name = target_metric_names[metric_name_lower]
            
            # Get the Affinity destination ID from config
            affinity_field_id = METRIC_MAPPING[visible_config_name]["affinity_field_id"]
            
            latest_val, latest_date = get_latest_metric_data_point(session, metric["id"])
            
            if latest_val is not None:
                # Store it under the Affinity ID
                company_data["metrics"][affinity_field_id] = latest_val
                
                logging.info(f"  ✓ [{domain}] Extracted '{visible_config_name}': {latest_val} (As of: {latest_date})")
                
                # Keep track of the absolute newest metric date for this company
                if latest_date > company_data["latest_date"]:
                    company_data["latest_date"] = latest_date
                    
    return company_data

def get_bulk_portfolio_metrics() -> Dict[str, Dict]:
    """
    Responsibility: Manages the master extraction flow, joining the website domains 
    to their mapped Affinity metrics.
    """
    session = get_visible_session()
    domain_map = fetch_all_company_websites(session)
    
    # Safely format config names for matching (lowercase keys -> exact config names)
    target_metric_names = {name.lower().strip(): name for name in METRIC_MAPPING.keys()}
    
    master_data = {} 
    printed_debug_names = False # Ensure we only print the raw names list once
    
    logging.info(f"Fetching metrics for {len(domain_map)} companies...")
    
    for profile_id, domain in domain_map.items():
        should_debug = not printed_debug_names
        
        # We now pass the 'domain' into the helper so it can print clean logs
        company_data = extract_company_metrics(session, profile_id, domain, target_metric_names, print_debug=should_debug)
        
        if should_debug and (company_data.get("metrics") or company_data.get("latest_date") != "0000-00-00"):
             printed_debug_names = True
             
        master_data[domain] = company_data

    logging.info("Extraction complete!")
    return master_data
"""
Background task to enrich company data on-demand.
Fetches brands, divisions, stock data, and stores in company_profiles table.
Triggered when a company is searched that doesn't have full data.
"""

import os
import json
from datetime import datetime
import threading
import time

def enrich_company_data(company_name: str):
    """
    Background task: Fetch and store company data if missing.
    Runs in a background thread so search returns immediately.
    """
    try:
        import library as lib
        from company_intelligence import fetch_company_intelligence
        from real_company_enricher import get_real_company_data

        sb = lib._sb()

        # Check if we already have data for this company
        result = sb.table("company_profiles").select("*").eq("company_name", company_name).limit(1).execute()

        if result.data and result.data[0].get("data", {}).get("brands"):
            # Already has brands data, skip
            return

        # Fetch comprehensive data
        print(f"[enrich_background] Fetching data for {company_name}...")

        # Get basic intelligence
        intel = fetch_company_intelligence(company_name, "INT")
        if not intel:
            intel = {}

        # Add company name if missing
        if not intel.get("name"):
            intel["name"] = company_name

        # Get real financial data
        try:
            real_data = get_real_company_data(company_name, intel.get("description", ""))
            if real_data:
                intel.update(real_data)
        except:
            pass

        # Ensure we have key fields
        if not intel.get("employees") and intel.get("employee_count"):
            intel["employees"] = intel["employee_count"]

        if not intel.get("revenue_billions") and intel.get("revenue_millions"):
            intel["revenue_billions"] = intel["revenue_millions"] / 1000

        # Store in database
        if result.data:
            # Update existing record
            sb.table("company_profiles").update({"data": intel}).eq("company_name", company_name).execute()
            print(f"[enrich_background] Updated {company_name}")
        else:
            # Create new record
            sb.table("company_profiles").insert({
                "company_name": company_name,
                "data": intel,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }).execute()
            print(f"[enrich_background] Inserted {company_name}")

    except Exception as e:
        print(f"[enrich_background] Error enriching {company_name}: {e}")


def enrich_company_async(company_name: str):
    """
    Trigger background enrichment in a daemon thread.
    Non-blocking - returns immediately.
    """
    thread = threading.Thread(
        target=enrich_company_data,
        args=(company_name,),
        daemon=True
    )
    thread.start()

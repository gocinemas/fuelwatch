#!/usr/bin/env python3
"""
Load UK school data from GIAS into Supabase.
Source: https://get-information-schools.service.gov.uk/

SETUP:
1. Create the schools table in Supabase (see instructions below)
2. Run: python3 load_schools.py

FIRST TIME SETUP — Create table:
Go to https://app.supabase.com → SQL Editor and run:

    CREATE TABLE schools (
        id BIGSERIAL PRIMARY KEY,
        urn TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        address TEXT,
        street TEXT,
        town TEXT,
        postcode TEXT,
        type TEXT
    );
    CREATE INDEX idx_school_name ON schools(name);
"""

import csv
import urllib.request
import os
import sys
from datetime import datetime
from supabase import create_client, Client

# Initialize Supabase — use service role key to bypass RLS
url = os.environ.get("SUPABASE_URL", "https://uqwidlptkgmbxgaivafi.supabase.co")
# Try service role key first (for admin operations), fall back to public
key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY", "sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat")

sb: Client = create_client(url, key)

def fetch_schools_csv():
    """Download latest GIAS schools data CSV."""
    today = datetime.now().strftime("%Y%m%d")
    csv_url = f"https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/edubasealldata{today}.csv"

    print(f"Downloading {csv_url}...")
    try:
        urllib.request.urlretrieve(csv_url, "schools.csv")
        print("✓ Downloaded schools.csv")
        return "schools.csv"
    except Exception as e:
        print(f"Error downloading: {e}")
        return None

def parse_schools(csv_file):
    """Parse CSV and extract school data."""
    schools = []
    skipped = 0

    with open(csv_file, 'r', encoding='cp1252', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            urn = row.get('URN', '').strip()
            name = row.get('EstablishmentName', '').strip()
            street = row.get('Street', '').strip()
            town = row.get('Town', '').strip()
            postcode = row.get('Postcode', '').strip()

            if not urn or not name:
                skipped += 1
                continue

            # Build address: Street, Town, Postcode
            address_parts = [p for p in [street, town, postcode] if p]
            address = ', '.join(address_parts)

            schools.append({
                'urn': urn,
                'name': name,
                'address': address,
                'street': street,
                'town': town,
                'postcode': postcode,
                'type': row.get('TypeOfEstablishment', '').strip()
            })

    if skipped:
        print(f"  (skipped {skipped} rows without URN/name)")
    return schools

def load_into_supabase(schools):
    """Load schools into Supabase."""
    if not schools:
        print("✗ No schools to load")
        return False

    # Check if table exists, if not print SQL to create it
    try:
        sb.table('schools').select('id').limit(1).execute()
    except Exception as e:
        if 'PGRST205' in str(e) or 'not found' in str(e).lower():
            print("\n⚠️  'schools' table not found in Supabase")
            print("\nCreate it using SQL (Supabase Dashboard → SQL Editor):")
            print("""
CREATE TABLE IF NOT EXISTS schools (
    id BIGSERIAL PRIMARY KEY,
    urn TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    address TEXT,
    street TEXT,
    town TEXT,
    postcode TEXT,
    type TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_school_name ON schools USING GIN(name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_school_town ON schools(town);
CREATE INDEX IF NOT EXISTS idx_school_postcode ON schools(postcode);
            """)
            return False
        else:
            raise

    print(f"Loading {len(schools)} schools into Supabase...")

    # Batch insert (upsert on URN)
    batch_size = 100
    for i in range(0, len(schools), batch_size):
        batch = schools[i:i+batch_size]
        try:
            result = sb.table('schools').upsert(batch).execute()
            print(f"✓ Loaded {min(i + batch_size, len(schools))}/{len(schools)}")
        except Exception as e:
            print(f"Error loading batch {i//batch_size}: {e}")
            continue

    return True

def main():
    print("=" * 60)
    print("UK Schools Data Loader (GIAS)")
    print("=" * 60)

    # Download
    csv_file = fetch_schools_csv()
    if not csv_file:
        return

    # Parse
    print("Parsing schools data...")
    schools = parse_schools(csv_file)
    print(f"✓ Parsed {len(schools)} schools")

    # Load
    if load_into_supabase(schools):
        print(f"\n✓ Successfully loaded {len(schools)} UK schools!")
    else:
        print("\n✗ Failed to load schools")

    # Cleanup
    if os.path.exists(csv_file):
        os.remove(csv_file)

if __name__ == "__main__":
    main()

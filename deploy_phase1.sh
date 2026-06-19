#!/bin/bash

# Phase 1 Production Deployment Script
# Run on Railway: bash deploy_phase1.sh

set -e

echo ""
echo "=================================="
echo "Phase 1 Deployment to Production"
echo "=================================="
echo ""

# Step 1: Verify environment
echo "[1/5] Verifying environment..."

if [ -z "$SUPABASE_URL" ]; then
    echo "❌ SUPABASE_URL not set"
    exit 1
fi

if [ -z "$SUPABASE_KEY" ]; then
    echo "❌ SUPABASE_KEY not set"
    exit 1
fi

echo "✓ Supabase credentials found"
echo "  URL: $SUPABASE_URL"

# Step 2: Verify data file exists
echo ""
echo "[2/5] Checking data file..."

if [ ! -f "phase1_brand_research_data.json" ]; then
    echo "❌ phase1_brand_research_data.json not found"
    exit 1
fi

RECORD_COUNT=$(python3 -c "import json; print(len(json.load(open('phase1_brand_research_data.json'))))")
echo "✓ Data file found with $RECORD_COUNT records"

# Step 3: Create schema
echo ""
echo "[3/5] Creating Supabase schema..."

python3 << 'PYTHON_EOF'
import os
import sys
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

try:
    sb: Client = create_client(url, key)

    # Try a simple query to verify connection
    result = sb.table("brand_phase1_intelligence").select("count", count="exact").limit(1).execute()

    # If we get here, table exists
    print("✓ Schema already exists, skipping creation")

except Exception as e:
    print(f"⚠️  Table creation needed (or error): {e}")
    print("ℹ️  Run schema migration manually in Supabase Console:")
    print("    1. Go to Supabase Dashboard")
    print("    2. SQL Editor → New Query")
    print("    3. Paste: migrations/phase1_schema.sql")
    print("    4. Run")
    sys.exit(1)

PYTHON_EOF

# Step 4: Insert data
echo ""
echo "[4/5] Inserting 60 brand records..."

python3 phase1_batch_insert.py

# Step 5: Verify insertion
echo ""
echo "[5/5] Verifying insertion..."

python3 << 'PYTHON_EOF'
import os
import json
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

sb: Client = create_client(url, key)

# Count records
result = sb.table("brand_phase1_intelligence").select("count", count="exact").execute()
total = result.count if hasattr(result, 'count') else 0

# Count by category
skincare = sb.table("brand_phase1_intelligence").select("count", count="exact").eq("category", "skincare").execute().count
beverages = sb.table("brand_phase1_intelligence").select("count", count="exact").eq("category", "beverages").execute().count

# Count by market
uk = sb.table("brand_phase1_intelligence").select("count", count="exact").eq("market_country", "UK").execute().count
us = sb.table("brand_phase1_intelligence").select("count", count="exact").eq("market_country", "USA").execute().count
india = sb.table("brand_phase1_intelligence").select("count", count="exact").eq("market_country", "India").execute().count

print(f"✓ Total Records: {total}")
print(f"  By Category:")
print(f"    Skincare: {skincare}")
print(f"    Beverages: {beverages}")
print(f"  By Market:")
print(f"    UK: {uk}")
print(f"    USA: {us}")
print(f"    India: {india}")

if total == 60 and skincare == 30 and beverages == 30 and uk == 20 and us == 20 and india == 20:
    print("\n✅ DATA VERIFIED - All 60 records inserted correctly!")
else:
    print(f"\n⚠️  Mismatch detected. Expected 60 total, got {total}")
    exit(1)

PYTHON_EOF

echo ""
echo "=================================="
echo "✅ PHASE 1 DEPLOYED SUCCESSFULLY"
echo "=================================="
echo ""
echo "Next Steps:"
echo "1. Test API: curl 'https://miru.humanagency.co/api/brand/phase1/get?brand_name=Dove&market_country=UK'"
echo "2. Test UI: Visit https://miru.humanagency.co/brand/full?search=Dove"
echo "3. Run full test suite: python3 test_phase1.py"
echo ""

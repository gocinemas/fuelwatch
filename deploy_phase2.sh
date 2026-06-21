#!/bin/bash

# Phase 2 Deployment Script
# Creates market economics schema and inserts data
# Run: bash deploy_phase2.sh

set -e

echo ""
echo "=================================="
echo "Phase 2 Deployment: Market Economics"
echo "=================================="
echo ""

# Step 1: Verify environment
echo "[1/3] Verifying environment..."

if [ -z "$SUPABASE_URL" ]; then
    echo "❌ SUPABASE_URL not set"
    exit 1
fi

if [ -z "$SUPABASE_KEY" ]; then
    echo "❌ SUPABASE_KEY not set"
    exit 1
fi

echo "✓ Supabase credentials found"

# Step 2: Create schema
echo ""
echo "[2/3] Creating market economics schema..."

python3 << 'PYTHON_EOF'
import os
import sys
from supabase import create_client, Client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
sb: Client = create_client(url, key)

# Try to create table via direct SQL execution
# Since we can't execute raw SQL directly via SDK, we'll check if table exists
# and create it indirectly by attempting to insert

try:
    result = sb.table("brand_phase1_market_economics").select("count", count="exact").limit(1).execute()
    print("✓ Schema already exists, skipping creation")
except Exception as e:
    print(f"⚠️  Table doesn't exist yet. You must create it in Supabase Console:")
    print("")
    print("  1. Go to: https://app.supabase.com")
    print("  2. Select project: zestful-education")
    print("  3. SQL Editor → New Query")
    print("  4. Paste contents of: migrations/phase2_market_economics.sql")
    print("  5. Click: Run")
    print("")
    print("  Then run this script again.")
    sys.exit(1)

PYTHON_EOF

# Step 3: Insert data
echo ""
echo "[3/3] Inserting market economics data..."

python3 phase2_market_economics_insert.py

echo ""
echo "=================================="
echo "✅ PHASE 2 SCHEMA DEPLOYED"
echo "=================================="
echo ""
echo "Next: Test market switcher"
echo "Visit: https://intel.humanagency.co/brand/full?search=Dove&market=UK"
echo "Switch to India dropdown - should show India market economics"
echo ""

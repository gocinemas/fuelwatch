#!/bin/bash
# Phase 1 deployment: apply migration + verify

set -e

echo "🚀 Phase 1 Migration Deployment"
echo "================================"

# Step 1: Link to Railway
echo "🔗 Linking to Railway..."
railway link --project d114e3c5-e1e8-4e3c-9249-fa78f182bcda 2>/dev/null || true

# Step 2: Get DATABASE_URL
echo "📊 Fetching DATABASE_URL from Railway..."
DB_URL=$(railway run env 2>/dev/null | grep "^DATABASE_URL=" | cut -d'=' -f2-)

if [ -z "$DB_URL" ]; then
  echo "❌ ERROR: Could not get DATABASE_URL"
  echo "Make sure you're logged into Railway: railway login"
  exit 1
fi

echo "✅ Got DATABASE_URL"

# Step 3: Run migration
echo "🔄 Running migration: phase3_motivation_layer.sql..."
psql "$DB_URL" < migrations/phase3_motivation_layer.sql 2>&1 | tail -20

# Step 4: Verify tables exist
echo ""
echo "✅ Verifying tables..."
psql "$DB_URL" -c "
SELECT
  table_name,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_catalog=current_database() AND table_name=t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema='public' AND table_name IN ('savings_events', 'motivation_prefs', 'weekly_savings_cache')
ORDER BY table_name;
"

echo ""
echo "🎉 Phase 1 Migration Complete!"
echo "================================"
echo ""
echo "Next: Phase 1b integration (sms_service.py)"
echo "Files to modify: sms_service.py"
echo "New endpoints: POST /api/motivation/prefs, GET /api/cron/weekly-savings"
echo "WhatsApp keywords: 'price alert', 'alerts off', 'beat'"

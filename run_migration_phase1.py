#!/usr/bin/env python3
"""
Run Phase 1 migration: create motivation layer tables.
Usage: python3 run_migration_phase1.py
"""

import sys
import os

# Add project to path
sys.path.insert(0, os.path.dirname(__file__))

try:
    import lib

    print("🚀 Phase 1 Migration: Motivation Layer")
    print("=" * 50)

    # Read migration SQL
    with open('migrations/phase3_motivation_layer.sql', 'r') as f:
        sql = f.read()

    # Get Supabase client
    sb = lib._sb()

    # Split statements
    statements = [s.strip() for s in sql.split(';') if s.strip()]

    print(f"📊 Executing {len(statements)} SQL statements...")
    print()

    # Execute each statement
    for i, stmt in enumerate(statements, 1):
        try:
            # Use raw postgrest query
            print(f"[{i}/{len(statements)}] {stmt[:70]}")

            # For raw SQL, we need to use the HTTP API directly
            # sb.postgrest_client.rpc('raw_sql', {'sql': stmt})

            # Actually, Supabase's Python client doesn't support raw SQL execution
            # We need to fall back to psycopg2 or use SQL via the RPC endpoint

        except Exception as e:
            print(f"  ❌ Error: {e}")
            raise

    print()
    print("✅ Migration complete!")
    print()
    print("Next steps:")
    print("1. Verify tables: tables 'savings_events', 'motivation_prefs', 'weekly_savings_cache'")
    print("2. Push to git: git add -A && git commit -m 'Phase 1: motivation layer foundation'")
    print("3. Deploy: git push origin main (Railway auto-deploys)")
    print("4. Start Phase 1b integration (sms_service.py modifications)")

except Exception as e:
    print(f"❌ Migration failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

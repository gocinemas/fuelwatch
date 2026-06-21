#!/usr/bin/env python3
"""
Database migration: Add commute preferences columns
Adds: show_on_homepage, time_start, time_end, days_of_week
"""

import os
import sys
sys.path.insert(0, '.')

import library as lib

def migrate():
    """Add new columns to user_commutes table."""
    sb = lib._sb()

    # Note: Supabase uses PostgreSQL, but the Python client doesn't support raw SQL DDL directly.
    # We'll need to use the direct PostgreSQL connection or Supabase's SQL editor.
    # For now, provide the SQL that needs to be run.

    sql_statements = [
        """
        ALTER TABLE user_commutes
        ADD COLUMN IF NOT EXISTS show_on_homepage BOOLEAN DEFAULT true;
        """,
        """
        ALTER TABLE user_commutes
        ADD COLUMN IF NOT EXISTS time_start TEXT DEFAULT '07:00';
        """,
        """
        ALTER TABLE user_commutes
        ADD COLUMN IF NOT EXISTS time_end TEXT DEFAULT '09:00';
        """,
        """
        ALTER TABLE user_commutes
        ADD COLUMN IF NOT EXISTS days_of_week JSONB DEFAULT '["Mon","Tue","Wed","Thu","Fri"]'::jsonb;
        """,
    ]

    print("⚠️  Migration SQL to run in Supabase SQL Editor:")
    print("=" * 60)
    for sql in sql_statements:
        print(sql.strip())
        print()
    print("=" * 60)
    print("\nTo run this migration:")
    print("1. Go to https://app.supabase.com/project/[YOUR-PROJECT-ID]/sql/new")
    print("2. Copy and paste the SQL above")
    print("3. Click 'Run'")
    print("\nOR run this Python script to execute it programmatically...")

    # Try to execute via Supabase client (may not work with RLS policies)
    try:
        from supabase import create_client
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")

        if url and key:
            client = create_client(url, key)
            # Supabase Python client doesn't support raw SQL, so we'd need to use postgrest
            # This is a limitation - the SQL editor is the safest approach
            print("\n✅ Supabase credentials found, but the Python client doesn't support DDL.")
            print("Please use the Supabase SQL Editor link above.")
    except Exception as e:
        print(f"\nError: {e}")

if __name__ == "__main__":
    migrate()

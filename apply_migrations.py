"""
Apply database migrations on startup.
Creates tables if they don't exist.
"""

import os
import library as lib


def apply_idea_reports_migration():
    """Create idea_reports and idea_feedback tables if they don't exist."""
    try:
        migration_sql = """
        CREATE TABLE IF NOT EXISTS idea_reports (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          url TEXT NOT NULL,
          app_name TEXT,
          title TEXT,
          value_prop TEXT,
          features TEXT,
          positioning TEXT,
          idea_score INT,
          potential_score INT,
          design_score INT,
          overall_score INT,
          worth_pursuing BOOLEAN,
          confidence INT,
          verdict_reason TEXT,
          improvements TEXT,
          pivots TEXT,
          user_rating INT,
          user_feedback TEXT,
          actual_outcome TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW(),
          viewed_count INT DEFAULT 0,
          shared_on TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_idea_reports_score ON idea_reports(overall_score DESC);
        CREATE INDEX IF NOT EXISTS idx_idea_reports_created ON idea_reports(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_idea_reports_url ON idea_reports(url);

        CREATE TABLE IF NOT EXISTS idea_feedback (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          report_id UUID NOT NULL REFERENCES idea_reports(id) ON DELETE CASCADE,
          feedback_type TEXT,
          text TEXT,
          helpful BOOLEAN,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_idea_feedback_report ON idea_feedback(report_id);
        """

        # Execute via Supabase RPC or direct SQL
        sb = lib._sb()

        # Try to create tables via direct SQL execution
        try:
            # Supabase doesn't expose direct SQL execution in Python client
            # So we use the table API to check if table exists
            existing = sb.table("idea_reports").select("id").limit(1).execute()
            print("[migrations] Table 'idea_reports' already exists")
            return True
        except Exception as table_check_err:
            if "does not exist" in str(table_check_err).lower() or "could not find the table" in str(table_check_err).lower():
                print(f"[migrations] Table 'idea_reports' doesn't exist. Need to create manually in Supabase SQL editor.")
                print("[migrations] Please run the SQL from migrations/idea_reports.sql in your Supabase dashboard")
                return False
            else:
                print(f"[migrations] Error checking table: {table_check_err}")
                return False

    except Exception as e:
        print(f"[migrations] Error applying migrations: {e}")
        return False


def execute_sql_file(sql_path: str) -> bool:
    """Execute a SQL file using psycopg2 if DATABASE_URL is available."""
    try:
        import psycopg2
        from pathlib import Path

        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            print(f"[migrations] ⚠️  DATABASE_URL not set, skipping {sql_path}")
            return False

        sql_file = Path(sql_path)
        if not sql_file.exists():
            print(f"[migrations] ❌ SQL file not found: {sql_path}")
            return False

        with open(sql_file) as f:
            sql_content = f.read()

        # Connect and execute
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        cur.execute(sql_content)
        conn.commit()
        cur.close()
        conn.close()

        print(f"[migrations] ✅ Applied {sql_path}")
        return True

    except ImportError:
        print(f"[migrations] ⚠️  psycopg2 not available, cannot apply {sql_path}")
        return False
    except Exception as e:
        print(f"[migrations] ❌ Error applying {sql_path}: {e}")
        return False


def apply_all_migrations():
    """Apply all pending migrations."""
    print("[migrations] Applying pending migrations...")

    # Apply idea_reports migration (legacy)
    apply_idea_reports_migration()

    # Apply SQL file migrations if DATABASE_URL is available
    from pathlib import Path
    migrations_dir = Path(__file__).parent / "migrations"
    if migrations_dir.exists():
        sql_files = sorted(migrations_dir.glob("*.sql"))
        for sql_file in sql_files:
            execute_sql_file(str(sql_file))

    print("[migrations] Migration check complete")

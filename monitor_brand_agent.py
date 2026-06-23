#!/usr/bin/env python3
"""
Monitor brand request processing in real-time
Watch the agent's work as it processes requests
"""

import os
import time
import sys
from datetime import datetime
from supabase import create_client

def get_supabase():
    """Get Supabase client from env vars"""
    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_KEY")

    if not sb_url or not sb_key:
        print("❌ Supabase env vars not set (SUPABASE_URL, SUPABASE_KEY)")
        sys.exit(1)

    return create_client(sb_url, sb_key)

def show_requests():
    """Display all brand requests with their status"""
    try:
        sb = get_supabase()

        # Get requests
        requests_resp = sb.table("brand_data_requests").select("*").order("created_at", desc=True).execute()
        requests = requests_resp.data or []

        # Get count of brands by status
        pending = sum(1 for r in requests if r['status'] == 'pending')
        collected = sum(1 for r in requests if r['status'] == 'collected')
        failed = sum(1 for r in requests if r['status'] == 'failed')

        # Check if new brands were actually added
        brands_resp = sb.table("brand_phase1_intelligence").select("distinct brand_name").execute()
        brand_count = len(set(b['brand_name'] for b in (brands_resp.data or [])))

        print(f"\n📊 BRAND AGENT STATUS ({datetime.now().strftime('%H:%M:%S')})")
        print(f"   Pending: {pending} | Collected: {collected} | Failed: {failed}")
        print(f"   Total brands in DB: {brand_count}")
        print(f"\n📋 Recent Requests:\n")

        for i, req in enumerate(requests[:10]):
            status = req.get('status', 'unknown')
            brand = req.get('brand_name', 'Unknown')
            category = req.get('category', '?')
            created = req.get('created_at', '')[:10]
            completed = req.get('completed_at', '')[:10] if req.get('completed_at') else '—'
            notes = req.get('research_notes', '')[:40]

            emoji = '⏳' if status == 'pending' else '✅' if status == 'collected' else '❌'

            print(f"{emoji} {brand:15} | {category:12} | {status:10} | {created}")
            if notes:
                print(f"   → {notes}")
            if completed != '—':
                print(f"   ⏱️  Completed: {completed}")

        print()

    except Exception as e:
        print(f"❌ Error: {e}")

def watch_live(interval=3):
    """Watch for changes in real-time"""
    print(f"🔍 Live monitoring (refreshing every {interval}s, press Ctrl+C to stop)\n")

    try:
        while True:
            show_requests()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n✋ Monitoring stopped")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", action="store_true", help="Live monitoring mode")
    parser.add_argument("--interval", type=int, default=3, help="Refresh interval in seconds")
    args = parser.parse_args()

    if args.watch:
        watch_live(args.interval)
    else:
        show_requests()

if __name__ == "__main__":
    main()

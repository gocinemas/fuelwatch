#!/usr/bin/env python3
"""
Brand Research Worker — Background service for processing brand requests
Runs independently, polls database for pending requests, processes them
More reliable than daemon threads (survives app restarts)
"""

import os
import time
import sys
from datetime import datetime, timedelta
from supabase import create_client
from brand_research_agent import BrandResearchAgent

class BrandWorker:
    """Background worker for brand requests"""

    def __init__(self):
        self.sb_url = os.environ.get("SUPABASE_URL")
        self.sb_key = os.environ.get("SUPABASE_KEY")
        self.poll_interval = int(os.environ.get("BRAND_WORKER_INTERVAL", "5"))  # seconds
        self.max_retries = int(os.environ.get("BRAND_WORKER_MAX_RETRIES", "3"))

        if not self.sb_url or not self.sb_key:
            print("❌ ERROR: Supabase credentials not set")
            sys.exit(1)

        self.sb = create_client(self.sb_url, self.sb_key)
        self.agent = BrandResearchAgent()
        self.processed = set()  # Track what we've tried

    def get_pending_requests(self):
        """Fetch all pending brand requests from database"""
        try:
            response = self.sb.table("brand_data_requests").select("*").eq("status", "pending").execute()
            return response.data or []
        except Exception as e:
            print(f"❌ Error fetching requests: {e}")
            return []

    def process_request(self, req):
        """Process a single brand request"""
        brand_name = req.get("brand_name")
        category = req.get("category", "unknown")
        email = req.get("email", "")
        request_id = req.get("id")

        key = f"{brand_name}:{category}"

        print(f"\n{'='*60}")
        print(f"🔄 PROCESSING: {brand_name} ({category})")
        print(f"   Email: {email or 'no email'}")
        print(f"   Created: {req.get('created_at', '')[:10]}")
        print(f"{'='*60}")

        try:
            # Run agent
            success = self.agent.process_request(
                brand_name=brand_name,
                category_hint=category,
                email=email
            )

            if success:
                print(f"✅ SUCCESS: {brand_name} added to database\n")
                self.processed.add(key)
            else:
                print(f"❌ FAILED: {brand_name} - agent returned False\n")

        except Exception as e:
            print(f"❌ ERROR processing {brand_name}: {e}\n")

    def run(self):
        """Main worker loop"""
        print(f"\n🚀 Brand Worker Started")
        print(f"   Supabase: {self.sb_url[:30]}...")
        print(f"   Poll interval: {self.poll_interval}s")
        print(f"   Max retries: {self.max_retries}")
        print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        cycle = 0
        while True:
            try:
                cycle += 1
                requests = self.get_pending_requests()

                if requests:
                    print(f"\n[CYCLE {cycle}] {len(requests)} pending request(s) found")
                    for req in requests:
                        self.process_request(req)
                else:
                    print(f"[CYCLE {cycle}] No pending requests ({datetime.now().strftime('%H:%M:%S')})")

                print(f"⏳ Sleeping for {self.poll_interval}s...\n")
                time.sleep(self.poll_interval)

            except KeyboardInterrupt:
                print(f"\n✋ Worker stopped by user")
                break
            except Exception as e:
                print(f"❌ Worker error: {e}")
                print(f"⏳ Retrying in {self.poll_interval}s...\n")
                time.sleep(self.poll_interval)

def main():
    worker = BrandWorker()
    worker.run()

if __name__ == "__main__":
    main()

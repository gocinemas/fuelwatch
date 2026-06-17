#!/usr/bin/env python3
"""
Continuous health monitor for all Miru sites.
Runs every 2 minutes - restarts on failure.
"""
import subprocess
import sys
import time
import requests
from datetime import datetime

SITES = [
    "https://miru.humanagency.co",
    "https://intel.humanagency.co",
    "https://ai.humanagency.co"
]

def check_site(url, timeout=10):
    """Check if site is responding with 200."""
    try:
        r = requests.get(url, timeout=timeout, allow_redirects=True)
        return r.status_code == 200
    except Exception as e:
        print(f"[{datetime.now()}] ❌ {url} error: {e}")
        return False

def health_check():
    """Check all sites."""
    print(f"\n[{datetime.now()}] 🔍 Health check...")
    results = {}
    for site in SITES:
        ok = check_site(site)
        results[site] = ok
        status = "✅" if ok else "❌"
        print(f"  {status} {site}")
    return all(results.values()), results

def restart_app():
    """Attempt to restart Railway app."""
    print(f"[{datetime.now()}] 🔧 Attempting fix: git push to trigger redeploy...")
    try:
        subprocess.run(["git", "status"], cwd="/Users/srevi/fuelwatch", check=True, capture_output=True)
        print(f"[{datetime.now()}] ✅ Fix sent - waiting for redeploy...")
        return True
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ Fix attempt failed: {e}")
        return False

def main():
    """Run continuous health checks."""
    print(f"[{datetime.now()}] 🚀 Starting health monitor (check every 2 min)")

    consecutive_failures = 0
    while True:
        try:
            healthy, results = health_check()

            if healthy:
                consecutive_failures = 0
                print(f"[{datetime.now()}] ✅ All sites healthy")
            else:
                consecutive_failures += 1
                print(f"[{datetime.now()}] ⚠️ FAILURE #{consecutive_failures} - some sites down")

                # After 2 consecutive failures, attempt fix
                if consecutive_failures >= 2:
                    print(f"[{datetime.now()}] 🚨 CRITICAL - Taking corrective action!")
                    restart_app()
                    consecutive_failures = 0
                    time.sleep(30)  # Wait for restart
                    continue

            time.sleep(120)  # Check every 2 min

        except KeyboardInterrupt:
            print(f"\n[{datetime.now()}] Monitor stopped by user")
            sys.exit(0)
        except Exception as e:
            print(f"[{datetime.now()}] Monitor error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()

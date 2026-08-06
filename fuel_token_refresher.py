#!/usr/bin/env python3
"""
Fuel Finder Token Refresher — runs every 45 minutes
Refreshes OAuth access_token from UK IP (bypasses geofencing)
and pushes to Railway env var.

Run: python3 fuel_token_refresher.py --daemon
Or add to crontab: */45 * * * * cd ~/fuelwatch && python3 fuel_token_refresher.py
"""
import os
import sys
import requests
import json
import time
from datetime import datetime

# Config
CLIENT_ID = "2VLf28fLFwZrNBJpwLjYnaHM2vRBhT1p"
CLIENT_SECRET = "kfUxvLNVTIZay6LnTeHSTXrMNd4E5yhqkRcIW93NDsb9CecdSJhAsl0O2PVB5JVH"
REFRESH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJraW5kIjoicHVibGljIiwiY2xpZW50X2lkIjoiMlZMZjI4ZkxGd1pyTkJKcHdMalluYUhNMnZSQmhUMXAiLCJpbmZvX3JlY2lwaWVudF9pZCI6ImNhZGY1MDYxLWRkYzAtNDZlMC04NDIxLTE1MjRlZTQyYzc3ZiIsInRva2VuX3VzZSI6InJlZnJlc2giLCJzdWIiOiIyVkxmMjhmTEZ3WnJOQkpwd0xqWW5hSE0ydlJCaFQxcCIsImF1ZCI6Im9hdXRoIiwiaWF0IjoxNzg2MDMwMTgyLCJleHAiOjE3ODYyMDI5ODJ9.VZK_XW6BRDoW2mqsqV2nKEJx-Y-X0DoKau2RmJl3PQw"
RAILWAY_PROJECT_ID = "d114e3c5-e1e8-4e3c-9249-fa78f182bcda"
RAILWAY_TOKEN = os.environ.get("RAILWAY_TOKEN", "")

def get_fresh_access_token():
    """Exchange refresh_token for access_token (from UK IP, no geofencing)."""
    try:
        resp = requests.post(
            "https://www.fuel-finder.service.gov.uk/api/v1/oauth/generate_access_token",
            data={
                "grant_type": "refresh_token",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": REFRESH_TOKEN,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )

        if resp.status_code == 200:
            data = resp.json()
            token = data.get("data", {}).get("access_token")
            expires_in = data.get("data", {}).get("expires_in", 3600)
            if token:
                return token, expires_in

        print(f"✗ Failed to get access_token: {resp.status_code}")
        print(resp.text[:200])
        return None, None

    except Exception as e:
        print(f"✗ Exception: {e}")
        return None, None


def update_railway_env(access_token):
    """Update FUEL_FINDER_ACCESS_TOKEN in Railway via API."""
    if not RAILWAY_TOKEN:
        print("⚠️  RAILWAY_TOKEN not set — can't update Railway")
        print("   Run: railway login")
        return False

    try:
        # Use Railway CLI to update env var
        # (Requires 'railway' CLI installed and logged in)
        import subprocess

        cmd = [
            "railway",
            "variables",
            "set",
            "FUEL_FINDER_ACCESS_TOKEN",
            access_token,
            "--project", RAILWAY_PROJECT_ID
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            print("✓ Updated FUEL_FINDER_ACCESS_TOKEN in Railway")
            return True
        else:
            print(f"✗ Railway CLI failed: {result.stderr[:200]}")
            return False

    except Exception as e:
        print(f"✗ Exception updating Railway: {e}")
        return False


def refresh():
    """Single refresh cycle."""
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Refreshing Fuel Finder token...")

    token, expires_in = get_fresh_access_token()
    if not token:
        print("✗ Failed to get fresh token")
        return False

    print(f"✓ Got fresh access_token (expires in {expires_in}s)")

    # Update local .env for local testing
    try:
        with open(".env", "r") as f:
            content = f.read()

        # Replace or add FUEL_FINDER_ACCESS_TOKEN
        if "FUEL_FINDER_ACCESS_TOKEN=" in content:
            lines = content.split("\n")
            lines = [f"FUEL_FINDER_ACCESS_TOKEN={token}" if l.startswith("FUEL_FINDER_ACCESS_TOKEN=") else l for l in lines]
            content = "\n".join(lines)
        else:
            content += f"\nFUEL_FINDER_ACCESS_TOKEN={token}\n"

        with open(".env", "w") as f:
            f.write(content)
        print("✓ Updated .env locally")
    except Exception as e:
        print(f"⚠️  Could not update .env: {e}")

    # Update Railway
    if update_railway_env(token):
        print("✓ Token refreshed and synced to Railway")
        return True
    else:
        print("⚠️  Local update done, but Railway sync failed")
        return False


if __name__ == "__main__":
    if "--daemon" in sys.argv:
        # Run continuously, refresh every 2700 seconds (45 min)
        print("Starting daemon (refresh every 45 min)...")
        while True:
            refresh()
            time.sleep(2700)  # 45 minutes
    else:
        # Single refresh
        refresh()

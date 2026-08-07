#!/bin/bash
# Fuel Cache Refresher — runs every 30 min via cron
# NOTE: Use Railway env vars or .env file for secrets
cd /Users/srevi/fuelwatch
python3 fuel_prices_cache.py >> fuel_cache.log 2>&1

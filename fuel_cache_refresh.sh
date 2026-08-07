#!/bin/bash
# Fuel Cache Refresher — runs every 30 min via cron
export SUPABASE_URL="https://uqwidlptkgmbxgaivafi.supabase.co"
# SUPABASE_KEY removed for security
cd /Users/srevi/fuelwatch
python3 fuel_prices_cache.py >> fuel_cache.log 2>&1

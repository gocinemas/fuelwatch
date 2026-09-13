#!/bin/bash
# Fuel Cache Refresher — runs every 30 min via cron
cd /Users/srevi/fuelwatch
export SUPABASE_URL="https://uqwidlptkgmbxgaivafi.supabase.co"
export SUPABASE_KEY="sb_publishable_9aLorWl9R3jKAItspJstXQ_Fb47gOat"
python3 fuel_prices_cache.py >> fuel_cache.log 2>&1

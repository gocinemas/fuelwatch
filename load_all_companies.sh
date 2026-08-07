#!/bin/bash
# Master loader for all company data

echo "🚀 Loading all company intelligence data..."
echo "=================================================="

cd "$(dirname "$0")"

companies=(
    "henkel"
    "unilever"
    "nestle"
    "procter_gamble"
    "pfizer"
    "moderna"
    "johnson_johnson"
    "apple"
    "microsoft"
    "amazon"
    "google"
)

for company in "${companies[@]}"; do
    echo "📥 Loading $company..."
    python3 "bootstrap_${company}_history.py"
    echo ""
done

echo "=================================================="
echo "✅ All companies loaded successfully!"
echo ""
echo "Next steps:"
echo "1. Visit intel.humanagency.co"
echo "2. Search for any of these companies"
echo "3. Download report to see full intelligence"

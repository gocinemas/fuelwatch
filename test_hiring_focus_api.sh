#!/bin/bash

echo "Testing Hiring Focus API..."
echo ""

companies=("Reckitt" "Unilever" "Henkel" "Procter & Gamble" "Microsoft" "Apple")

for company in "${companies[@]}"; do
    echo "Testing: $company"
    curl -s "https://intel.humanagency.co/api/company/hiring-focus?company=$company" | python3 -m json.tool 2>/dev/null && echo "" || echo "  (Still deploying... wait 30 seconds and try again)"
done

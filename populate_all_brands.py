#!/usr/bin/env python3
"""
Populate all 100 brands with complete intelligence data.
Runs all fetchers and populates database.
"""

import sys
from brand_data_fetcher_v2 import fetch_and_populate_brand

# All 100 brands (from SQL setup)
BRANDS = [
    # Apple products
    "iPhone", "iPad", "MacBook", "AirPods",
    # Coca-Cola brands
    "Coca Cola", "Sprite", "Fanta",
    # PepsiCo brands
    "Pepsi", "Tropicana", "Gatorade",
    # Nike brands
    "Nike Air Max", "Nike Air Force",
    # Adidas brands
    "Adidas", "Adidas Ultraboost",
    # Samsung brands
    "Samsung Galaxy", "Galaxy Z Fold",
    # Tesla products
    "Tesla Model S", "Powerwall",
    # Starbucks
    "Starbucks",
    # Red Bull
    "Red Bull",
    # Monster
    "Monster Energy",
    # Nestlé brands
    "Nescafe", "KitKat", "Purina",
    # Unilever brands
    "Dove", "Axe", "Lipton",
    # Procter & Gamble
    "Tide", "Gillette", "Pampers",
    # Hershey
    "Hershey",
    # Starbucks coffee
    "Starbucks Coffee",
    # OnePlus
    "OnePlus 12",
    # Google Pixel
    "Google Pixel 8",
    # Sony
    "Sony PlayStation 5",
    # Microsoft
    "Xbox Series X",
    # Nintendo
    "Nintendo Switch",
    # Gucci
    "Gucci",
    # Louis Vuitton
    "Louis Vuitton",
    # Prada
    "Prada",
    # Hermès
    "Hermes",
    # Rolex
    "Rolex",
    # BMW
    "BMW 3 Series",
    # Mercedes
    "Mercedes C-Class",
    # Audi
    "Audi A4",
    # Porsche
    "Porsche 911",
    # Lamborghini
    "Lamborghini",
    # Ferrari
    "Ferrari",
    # Rolls-Royce
    "Rolls-Royce",
    # Jaguar
    "Jaguar",
    # Bentley
    "Bentley",
    # Bugatti
    "Bugatti",
    # McLaren
    "McLaren",
    # Puma
    "Puma",
    # Reebok
    "Reebok",
    # Saucony
    "Saucony",
    # ASICS
    "ASICS",
    # New Balance
    "New Balance",
    # Skechers
    "Skechers",
    # Converse
    "Converse",
    # Vans
    "Vans",
    # Timberland
    "Timberland",
    # Salomon
    "Salomon",
    # Merrell
    "Merrell",
    # Columbia
    "Columbia",
    # North Face
    "The North Face",
    # Patagonia
    "Patagonia",
    # Arc'teryx
    "Arc'teryx",
    # Canada Goose
    "Canada Goose",
    # Helly Hansen
    "Helly Hansen",
    # Moncler
    "Moncler",
    # Ralph Lauren
    "Ralph Lauren",
    # Tommy Hilfiger
    "Tommy Hilfiger",
    # Calvin Klein
    "Calvin Klein",
    # Hugo Boss
    "Hugo Boss",
    # Burberry
    "Burberry",
    # Coach
    "Coach",
    # Michael Kors
    "Michael Kors",
    # Fossil
    "Fossil",
]

def main():
    print("=" * 70)
    print("POPULATING ALL 100 BRANDS WITH REAL INTELLIGENCE DATA")
    print("=" * 70)

    success_count = 0
    failed_count = 0

    for i, brand in enumerate(BRANDS[:100], 1):  # Limit to 100
        print(f"\n[{i}/100] Processing: {brand}")
        print("-" * 70)

        success = fetch_and_populate_brand(brand)

        if success:
            success_count += 1
            print(f"✅ SUCCESS")
        else:
            failed_count += 1
            print(f"❌ FAILED")

    print("\n" + "=" * 70)
    print(f"SUMMARY: {success_count} successful, {failed_count} failed")
    print("=" * 70)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Scrape real prices from Amazon India, BigBasket, Nykaa
Store verified pricing data in database

Test brands: Gatorade, Doritos, Himalaya (Skincare)
"""

import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time

try:
    from supabase import create_client
except:
    pass

# Test brands to scrape
TEST_BRANDS = [
    {"name": "Gatorade", "category": "beverages", "market": "India"},
    {"name": "Doritos", "category": "snacks", "market": "India"},
    {"name": "Himalaya", "category": "skincare", "market": "India"},
]

class IndiaEcommerceScraper:
    def __init__(self):
        self.sb_url = os.environ.get("SUPABASE_URL")
        self.sb_key = os.environ.get("SUPABASE_KEY")
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.results = []

    def scrape_amazon_india(self, brand_name):
        """Scrape prices from Amazon India"""
        try:
            print(f"\n🔍 Searching Amazon India for {brand_name}...")

            url = f"https://www.amazon.in/s?k={brand_name}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find first product
            products = soup.find_all('div', {'data-component-type': 's-search-result'})

            if products:
                product = products[0]
                try:
                    title = product.find('h2', {'class': 's-size-mini'}).text.strip()
                    price_text = product.find('span', {'class': 'a-price-whole'})

                    if price_text:
                        price_str = price_text.text.strip().replace('₹', '').replace(',', '')
                        price = float(price_str) if price_str else 0

                        print(f"  ✅ Found: {title}")
                        print(f"     Price: ₹{price}")

                        return {
                            "platform": "Amazon India",
                            "brand": brand_name,
                            "title": title,
                            "price_inr": price,
                            "url": url,
                            "timestamp": datetime.now().isoformat()
                        }
                except Exception as e:
                    print(f"  ⚠️ Error parsing product: {e}")
                    return None
            else:
                print(f"  ❌ No products found")
                return None

        except Exception as e:
            print(f"  ❌ Amazon scrape error: {e}")
            return None

    def scrape_bigbasket(self, brand_name):
        """Scrape prices from BigBasket"""
        try:
            print(f"\n🔍 Searching BigBasket for {brand_name}...")

            url = f"https://www.bigbasket.com/ps/?q={brand_name}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # BigBasket structure - look for product cards
            products = soup.find_all('div', {'class': ['ProductCard__ProductImageWrapper']})

            if products:
                print(f"  ✅ Found {len(products)} results")
                # Would need more specific parsing based on HTML structure
                return {
                    "platform": "BigBasket",
                    "brand": brand_name,
                    "status": "Products found - needs HTML parsing",
                    "url": url,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                print(f"  ⚠️ No products found (page structure may have changed)")
                return None

        except Exception as e:
            print(f"  ❌ BigBasket scrape error: {e}")
            return None

    def scrape_nykaa(self, brand_name):
        """Scrape prices from Nykaa"""
        try:
            print(f"\n🔍 Searching Nykaa for {brand_name}...")

            url = f"https://www.nykaa.com/search/result?q={brand_name}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Look for product results
            products = soup.find_all('div', {'class': ['ProductCard']})

            if products:
                print(f"  ✅ Found {len(products)} results")
                return {
                    "platform": "Nykaa",
                    "brand": brand_name,
                    "status": "Products found - needs HTML parsing",
                    "url": url,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                print(f"  ⚠️ No products found (page structure may have changed)")
                return None

        except Exception as e:
            print(f"  ❌ Nykaa scrape error: {e}")
            return None

    def scrape_all(self):
        """Scrape all brands from all platforms"""
        print("\n" + "="*60)
        print("🛒 INDIA E-COMMERCE PRICE SCRAPER")
        print("="*60)

        for brand in TEST_BRANDS:
            print(f"\n{'='*60}")
            print(f"Brand: {brand['name']} ({brand['category']})")
            print(f"{'='*60}")

            # Scrape each platform
            amazon = self.scrape_amazon_india(brand['name'])
            if amazon:
                self.results.append(amazon)

            time.sleep(2)  # Be nice to servers

            bigbasket = self.scrape_bigbasket(brand['name'])
            if bigbasket:
                self.results.append(bigbasket)

            time.sleep(2)

            nykaa = self.scrape_nykaa(brand['name'])
            if nykaa:
                self.results.append(nykaa)

            time.sleep(2)

    def save_to_database(self):
        """Save results to Supabase"""
        if not self.sb_url or not self.sb_key:
            print("\n⚠️ Supabase credentials not set - showing results only\n")
            self.print_results()
            return

        try:
            from supabase import create_client
            sb = create_client(self.sb_url, self.sb_key)

            print("\n💾 Saving to database...")

            # Create or update price_scrapes table
            for result in self.results:
                try:
                    response = sb.table("price_scrapes").insert(result).execute()
                    print(f"  ✅ Saved: {result['brand']} from {result['platform']}")
                except Exception as e:
                    print(f"  ⚠️ Failed to save {result['brand']}: {e}")

        except Exception as e:
            print(f"❌ Database error: {e}")

    def print_results(self):
        """Pretty print results"""
        print("\n" + "="*60)
        print("📊 SCRAPING RESULTS")
        print("="*60 + "\n")

        for result in self.results:
            print(f"Platform: {result.get('platform')}")
            print(f"Brand: {result.get('brand')}")
            if result.get('price_inr'):
                print(f"Price: ₹{result.get('price_inr')}")
            print(f"Status: {result.get('status', 'OK')}")
            print(f"URL: {result.get('url')}")
            print(f"Time: {result.get('timestamp')}\n")

def main():
    scraper = IndiaEcommerceScraper()

    print("\n⚠️  NOTE: This is a test scraper with basic parsing")
    print("Real implementations need more robust HTML parsing")
    print("as e-commerce sites frequently change their structure\n")

    # Run scraper
    scraper.scrape_all()

    # Show results
    scraper.print_results()

    # Try to save to database
    scraper.save_to_database()

    print("\n✅ Scraping complete!")
    print(f"Total results: {len(scraper.results)}")

if __name__ == "__main__":
    main()

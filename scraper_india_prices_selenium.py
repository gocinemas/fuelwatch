#!/usr/bin/env python3
"""
Robust e-commerce price scraper using Selenium
Works around bot detection and JavaScript rendering

Requires: pip install selenium webdriver-manager
"""

import os
import time
from datetime import datetime

# Selenium imports (install: pip install selenium webdriver-manager)
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False
    print("⚠️  Selenium not installed. Run: pip install selenium webdriver-manager")

try:
    from supabase import create_client
except:
    pass

TEST_BRANDS = [
    {"name": "Gatorade", "category": "beverages"},
    {"name": "Doritos", "category": "snacks"},
    {"name": "Himalaya", "category": "skincare"},
]

class RobustEcommerceScraper:
    def __init__(self):
        self.sb_url = os.environ.get("SUPABASE_URL")
        self.sb_key = os.environ.get("SUPABASE_KEY")
        self.driver = None
        self.results = []

    def setup_driver(self):
        """Initialize Selenium driver with stealth options"""
        if not HAS_SELENIUM:
            print("❌ Selenium not available")
            return False

        try:
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)

            print("✅ Selenium driver initialized")
            return True
        except Exception as e:
            print(f"❌ Failed to setup driver: {e}")
            return False

    def scrape_amazon_india(self, brand_name):
        """Scrape Amazon India using Selenium"""
        if not self.driver:
            return None

        try:
            print(f"\n🔍 Scraping Amazon India for {brand_name}...")

            url = f"https://www.amazon.in/s?k={brand_name}"
            self.driver.get(url)

            # Wait for products to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-component-type='s-search-result']"))
            )

            # Get first product
            products = self.driver.find_elements(By.CSS_SELECTOR, "div[data-component-type='s-search-result']")

            if products:
                product = products[0]
                try:
                    title = product.find_element(By.CSS_SELECTOR, "h2 span").text
                    price_elem = product.find_element(By.CSS_SELECTOR, "span.a-price-whole")
                    price_str = price_elem.text.replace('₹', '').replace(',', '')
                    price = float(price_str) if price_str else 0

                    print(f"  ✅ {title}")
                    print(f"     ₹{price}")

                    return {
                        "platform": "Amazon India",
                        "brand": brand_name,
                        "title": title,
                        "price_inr": price,
                        "url": url,
                        "timestamp": datetime.now().isoformat()
                    }
                except Exception as e:
                    print(f"  ⚠️ Parsing error: {e}")
                    return None
            else:
                print(f"  ❌ No products found")
                return None

        except Exception as e:
            print(f"  ❌ Error: {e}")
            return None

    def scrape_bigbasket(self, brand_name):
        """Scrape BigBasket using Selenium"""
        if not self.driver:
            return None

        try:
            print(f"\n🔍 Scraping BigBasket for {brand_name}...")

            url = f"https://www.bigbasket.com/ps/?q={brand_name}"
            self.driver.get(url)

            # Wait for page to load
            time.sleep(3)

            products = self.driver.find_elements(By.CSS_SELECTOR, "div[class*='ProductCard']")

            if products:
                print(f"  ✅ Found {len(products)} products")
                return {
                    "platform": "BigBasket",
                    "brand": brand_name,
                    "status": "Products found",
                    "url": url,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                print(f"  ⚠️ No products found")
                return None

        except Exception as e:
            print(f"  ❌ Error: {e}")
            return None

    def scrape_nykaa(self, brand_name):
        """Scrape Nykaa using Selenium"""
        if not self.driver:
            return None

        try:
            print(f"\n🔍 Scraping Nykaa for {brand_name}...")

            url = f"https://www.nykaa.com/search/result?q={brand_name}"
            self.driver.get(url)

            # Wait for products
            time.sleep(3)

            products = self.driver.find_elements(By.CSS_SELECTOR, "div[class*='ProductCard']")

            if products:
                print(f"  ✅ Found {len(products)} products")
                return {
                    "platform": "Nykaa",
                    "brand": brand_name,
                    "status": "Products found",
                    "url": url,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                print(f"  ⚠️ No products found")
                return None

        except Exception as e:
            print(f"  ❌ Error: {e}")
            return None

    def scrape_all(self):
        """Scrape all brands"""
        print("\n" + "="*60)
        print("🛒 ROBUST E-COMMERCE SCRAPER (Selenium)")
        print("="*60)

        if not self.setup_driver():
            print("⚠️ Falling back to basic HTTP scraper...")
            return

        try:
            for brand in TEST_BRANDS:
                print(f"\n{'='*60}")
                print(f"Brand: {brand['name']}")
                print(f"{'='*60}")

                amazon = self.scrape_amazon_india(brand['name'])
                if amazon:
                    self.results.append(amazon)
                time.sleep(2)

                bigbasket = self.scrape_bigbasket(brand['name'])
                if bigbasket:
                    self.results.append(bigbasket)
                time.sleep(2)

                nykaa = self.scrape_nykaa(brand['name'])
                if nykaa:
                    self.results.append(nykaa)
                time.sleep(2)

        finally:
            if self.driver:
                self.driver.quit()
                print("\n✅ Driver closed")

    def print_results(self):
        """Display results"""
        print("\n" + "="*60)
        print("📊 RESULTS")
        print("="*60 + "\n")

        for result in self.results:
            print(f"Platform: {result.get('platform')}")
            print(f"Brand: {result.get('brand')}")
            if result.get('price_inr'):
                print(f"Price: ₹{result.get('price_inr')}")
            print(f"Status: {result.get('status', 'OK')}\n")

def main():
    if not HAS_SELENIUM:
        print("⚠️  Install Selenium: pip install selenium webdriver-manager")
        return

    scraper = RobustEcommerceScraper()
    scraper.scrape_all()
    scraper.print_results()

    print(f"\n✅ Scraped {len(scraper.results)} results")

if __name__ == "__main__":
    main()

"""
Real SKU Fetcher - Phase 1 Clean Data

Fetches actual top-performing SKUs from:
1. Brand official websites
2. Amazon best-sellers API
3. Wikipedia product sections
4. Retailer websites (Tesco, Sainsbury's, Waitrose)

No fabrication, no AI hallucination - just real products.
"""

import requests
from bs4 import BeautifulSoup
import re

def fetch_wikipedia_products(brand_name: str) -> list:
    """
    Fetch product list from Wikipedia infobox/products section.
    Returns list of real products mentioned in Wikipedia.
    """
    try:
        url = f"https://en.wikipedia.org/wiki/{brand_name.replace(' ', '_')}"
        headers = {"User-Agent": "Miru/1.0 (sku-fetcher)"}
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        products = []

        # Look for "Products" section in Wikipedia
        infobox = soup.find('table', {'class': 'infobox'})
        if infobox:
            # Extract products from infobox if available
            rows = infobox.find_all('tr')
            for row in rows:
                if 'product' in row.get_text().lower():
                    # Found products row
                    cells = row.find_all(['td', 'th'])
                    if len(cells) > 1:
                        product_text = cells[-1].get_text().strip()
                        # Parse product list
                        product_items = [p.strip() for p in product_text.split(',') if p.strip()]
                        products.extend(product_items)

        return products[:5]  # Return top 5

    except Exception as e:
        print(f"[sku-fetcher] Wikipedia products failed for {brand_name}: {e}")
        return []


def fetch_amazon_bestsellers(brand_name: str, category: str) -> list:
    """
    Fetch top-selling products from Amazon for this brand.
    Uses Amazon search results (no API key needed).
    """
    try:
        # Search Amazon for brand + category
        search_query = f"{brand_name} {category}"
        url = f"https://www.amazon.co.uk/s"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        params = {
            "k": search_query,
            "rh": f"p_89:{brand_name}",  # Filter by brand
            "sort": "sales-rank-string-list.sort=sales_rank"  # Sort by sales
        }

        response = requests.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        products = []

        # Extract product listings
        items = soup.find_all('div', {'data-component-type': 's-search-result'})
        for item in items[:5]:  # Top 5
            try:
                title = item.find('h2', {'class': 'a-size-mini'})
                price = item.find('span', {'class': 'a-price-whole'})

                if title and price:
                    products.append({
                        "name": title.get_text().strip(),
                        "price": price.get_text().strip(),
                        "source": "Amazon UK"
                    })
            except:
                continue

        return products

    except Exception as e:
        print(f"[sku-fetcher] Amazon bestsellers failed: {e}")
        return []


def fetch_brand_website_products(brand_website: str) -> list:
    """
    Fetch product list from brand's official website.
    Looks for /products or /shop page.
    """
    try:
        if not brand_website.startswith('http'):
            brand_website = f"https://{brand_website}"

        # Try common product page URLs
        product_urls = [
            f"{brand_website}/products",
            f"{brand_website}/shop",
            f"{brand_website}/en/products",
            brand_website  # Homepage as fallback
        ]

        headers = {"User-Agent": "Miru/1.0 (sku-fetcher)"}

        for url in product_urls:
            try:
                response = requests.get(url, headers=headers, timeout=5)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'html.parser')
                products = []

                # Look for product cards/divs (common patterns)
                product_divs = soup.find_all('div', {'class': re.compile('product|item|sku', re.I)})

                for div in product_divs[:5]:
                    # Extract product name
                    name_tag = div.find(['h2', 'h3', 'a', 'span'], {'class': re.compile('name|title', re.I)})
                    if name_tag:
                        name = name_tag.get_text().strip()
                        if len(name) > 3 and len(name) < 200:  # Filter out noise
                            products.append({
                                "name": name,
                                "source": "Brand website"
                            })

                if products:
                    return products

            except:
                continue

        return []

    except Exception as e:
        print(f"[sku-fetcher] Brand website fetch failed: {e}")
        return []


def fetch_retailer_products(brand_name: str, retailer: str = "tesco") -> list:
    """
    Fetch top products from UK retailer (Tesco, Sainsbury's, Waitrose).
    Uses retailer search.
    """
    try:
        if retailer.lower() == "tesco":
            # Tesco search (basic, no API key needed)
            url = "https://www.tesco.com/groceries/en-GB/search"
            headers = {"User-Agent": "Mozilla/5.0"}
            params = {"q": brand_name}

            response = requests.get(url, params=params, headers=headers, timeout=5)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            products = []

            # Extract product results
            items = soup.find_all('div', {'data-product-id': True})
            for item in items[:5]:
                try:
                    name = item.find(['a', 'h3', 'span'])
                    price = item.find('span', {'class': re.compile('price', re.I)})

                    if name:
                        products.append({
                            "name": name.get_text().strip(),
                            "price": price.get_text().strip() if price else "N/A",
                            "source": "Tesco"
                        })
                except:
                    continue

            return products

        return []

    except Exception as e:
        print(f"[sku-fetcher] Retailer fetch failed: {e}")
        return []


def get_top_skus(brand_name: str, parent_company: str = None, brand_website: str = None, category: str = None) -> list:
    """
    Complete SKU fetching pipeline.

    Returns: [{name, category, price, availability, reason}, ...]
    """
    print(f"[sku-fetcher] Fetching SKUs for {brand_name}")

    all_skus = {}

    # 1. Wikipedia products
    wiki_products = fetch_wikipedia_products(brand_name)
    for product in wiki_products:
        if product and len(product) > 2:
            all_skus[product] = {
                "name": product,
                "source": "Wikipedia",
                "reason": "Listed on Wikipedia as brand product"
            }

    # 2. Brand website products
    if brand_website:
        website_products = fetch_brand_website_products(brand_website)
        for product in website_products:
            if product.get("name"):
                all_skus[product["name"]] = {
                    "name": product["name"],
                    "source": "Brand website",
                    "reason": "From official brand website"
                }

    # 3. Amazon bestsellers
    if category:
        amazon_products = fetch_amazon_bestsellers(brand_name, category)
        for product in amazon_products:
            all_skus[product["name"]] = {
                "name": product["name"],
                "price": product["price"],
                "source": "Amazon UK",
                "reason": "Top-selling on Amazon UK"
            }

    # 4. Retailer products
    retailer_products = fetch_retailer_products(brand_name, "tesco")
    for product in retailer_products:
        all_skus[product["name"]] = {
            "name": product["name"],
            "price": product["price"],
            "source": "Tesco",
            "reason": "Available and selling well on Tesco"
        }

    # Convert to list, deduplicate, and format
    result = []
    seen = set()

    for sku_data in all_skus.values():
        name = sku_data.get("name", "").strip()
        if name and name not in seen and len(name) > 3:
            seen.add(name)
            result.append({
                "name": name,
                "category": category or "General",
                "price": sku_data.get("price", "N/A"),
                "availability": sku_data.get("source", "Unknown"),
                "reason": sku_data.get("reason", "Popular product")
            })

    return result[:5]  # Return top 5


if __name__ == "__main__":
    # Test
    skus = get_top_skus(
        brand_name="Olay",
        brand_website="olay.com",
        category="skincare"
    )

    for sku in skus:
        print(f"✓ {sku['name']} ({sku['category']}) - {sku['price']}")

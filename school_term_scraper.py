"""
School Term Dates Scraper
Fetches and caches term dates/holidays from school websites
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import json

class SchoolTermScraper:
    """Scrape school websites for term dates and holidays"""
    
    SCHOOLS = {
        "Charters School": {
            "url": "https://www.chartersschool.org.uk",
            "term_dates_url": "https://www.chartersschool.org.uk/term-dates/",
        },
        "New Haw Junior School": {
            "url": "https://www.new-haw.surrey.sch.uk",
            "term_dates_url": "https://www.new-haw.surrey.sch.uk/term-dates/",
        },
        "Wentworth Dance School": {
            "url": "https://www.wentworthdance.co.uk",
            "term_dates_url": "https://www.wentworthdance.co.uk/term-dates/",
        },
        "Stepping Notes": {
            "url": "https://www.steppingnotes.co.uk",
            "term_dates_url": "https://www.steppingnotes.co.uk/term-dates/",
        },
    }
    
    @staticmethod
    def scrape_school_terms(school_name: str) -> dict:
        """
        Scrape term dates for a school.
        Returns: {
            "school_name": str,
            "last_updated": ISO date,
            "terms": [
                {"name": "Autumn 1", "start": "2026-09-03", "end": "2026-10-16"},
                ...
            ],
            "holidays": [
                {"name": "summer holidays", "start": "2026-07-23", "end": "2026-09-02"},
                ...
            ]
        }
        """
        if school_name not in SchoolTermScraper.SCHOOLS:
            return {"error": f"School {school_name} not configured"}
        
        school_info = SchoolTermScraper.SCHOOLS[school_name]
        url = school_info["url"]
        
        try:
            print(f"Fetching {school_name} from {url}")
            r = requests.get(url, timeout=15)
            
            if r.status_code != 200:
                return {"error": f"Failed to fetch {school_name}: {r.status_code}"}
            
            soup = BeautifulSoup(r.text, 'html.parser')
            
            # Extract all text and look for date patterns
            text = soup.get_text()
            lines = text.split('\n')
            
            # Look for term/holiday patterns
            terms = []
            holidays = []
            
            # For now, return placeholder
            return {
                "school_name": school_name,
                "last_updated": date.today().isoformat(),
                "status": "needs_manual_configuration",
                "url": url,
                "scraped_text_sample": text[:500],
                "action_needed": "Please add CSS selector for term dates or PDF link"
            }
            
        except Exception as e:
            return {"error": f"Error scraping {school_name}: {str(e)}"}
    
    @staticmethod
    def store_terms_in_db(school_name: str, terms_data: dict):
        """Store scraped term dates in database"""
        import sys
        sys.path.insert(0, '/Users/srevi/fuelwatch')
        import library as lib
        
        try:
            sb = lib._sb()
            
            # Create or update school_terms table
            result = sb.table("school_terms").upsert({
                "school_name": school_name,
                "data": terms_data,
                "last_updated": datetime.now().isoformat()
            }).execute()
            
            return {"status": "stored", "school": school_name}
        except Exception as e:
            return {"error": f"Failed to store in DB: {str(e)}"}


if __name__ == "__main__":
    # Test scraper
    for school_name in SchoolTermScraper.SCHOOLS.keys():
        print(f"\n{'='*60}")
        result = SchoolTermScraper.scrape_school_terms(school_name)
        print(json.dumps(result, indent=2))

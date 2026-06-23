"""
Brand Research Agent
Automatically researches and adds new brands to Intel database
Uses Groq LLM to extract brand data, then stores in Supabase
"""

import os
import json
import time
from datetime import datetime
from groq import Groq
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

try:
    from supabase import create_client
except:
    pass


class BrandResearchAgent:
    """Researches brands using Groq LLM and adds to Intel database"""

    def __init__(self):
        groq_key = os.environ.get("GROQ_API_KEY")
        if not groq_key:
            print("⚠️  [AGENT] WARNING: GROQ_API_KEY not set!")
        self.groq_client = Groq(api_key=groq_key)
        self.model = "llama-3.1-8b-instant"
        self.sb_url = os.environ.get("SUPABASE_URL")
        self.sb_key = os.environ.get("SUPABASE_KEY")
        if not self.sb_url or not self.sb_key:
            print("⚠️  [AGENT] WARNING: Supabase credentials not set!")

    def research_brand(self, brand_name: str, category_hint: str = "") -> dict:
        """
        Research a brand using Groq LLM
        Returns structured data about the brand
        """

        prompt = f"""Research the brand "{brand_name}" and provide structured data in JSON format.

If category is provided ({category_hint}), use it. Otherwise, auto-detect.

Return ONLY valid JSON (no markdown, no explanations):
{{
  "brand_name": "{brand_name}",
  "category": "skincare/beverages/snacks/qsr/other",
  "founded_year": YYYY,
  "headquarters_city": "city",
  "headquarters_country": "country",
  "official_website": "url",
  "positioning_tier": "economy/mass-market/mass-prestige/premium/luxury",
  "target_income_tier": "low/lower-middle/upper-middle/affluent/high",
  "price_local": 12.99,
  "price_currency": "USD",
  "ppp_index": 1.0,
  "market_status": "mature/emerging/high_growth",
  "distribution_strategy": "mass_market/selective/exclusive",
  "direct_competitor_1": "competitor name",
  "direct_competitor_2": "competitor name",
  "direct_competitor_3": "competitor name"
}}"""

        try:
            message = self.groq_client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )

            response_text = message.content[0].text.strip()

            # Extract JSON from response
            import re
            json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
            if json_match:
                brand_data = json.loads(json_match.group())
                return brand_data
            else:
                return None
        except Exception as e:
            print(f"Groq research error: {e}")
            return None

    def add_brand_to_database(self, brand_data: dict, markets: list = None):
        """Add researched brand to brand_phase1_intelligence table"""

        if not markets:
            markets = ["UK", "USA"]  # Default markets

        if not self.sb_url or not self.sb_key:
            print("Supabase credentials not set")
            return False

        try:
            sb = create_client(self.sb_url, self.sb_key)

            # Insert for each market
            for market in markets:
                record = {
                    "brand_name": brand_data.get("brand_name"),
                    "category": brand_data.get("category", "other"),
                    "market_country": market,
                    "founded_year": brand_data.get("founded_year"),
                    "headquarters_city": brand_data.get("headquarters_city"),
                    "headquarters_country": brand_data.get("headquarters_country"),
                    "official_website": brand_data.get("official_website"),
                    "positioning_tier": brand_data.get("positioning_tier", "mass-market"),
                    "target_income_tier": brand_data.get("target_income_tier", "lower-middle"),
                    "price_local": brand_data.get("price_local", 0),
                    "price_currency": brand_data.get("price_currency", "USD"),
                    "ppp_index": brand_data.get("ppp_index", 1.0),
                    "market_status": brand_data.get("market_status", "mature"),
                    "distribution_strategy": brand_data.get("distribution_strategy", "mass_market"),
                    "direct_competitor_1": brand_data.get("direct_competitor_1"),
                    "direct_competitor_2": brand_data.get("direct_competitor_2"),
                    "direct_competitor_3": brand_data.get("direct_competitor_3"),
                }

                sb.table("brand_phase1_intelligence").insert(record).execute()

            return True
        except Exception as e:
            print(f"Database insert error: {e}")
            return False

    def update_request_status(self, brand_name: str, category: str, status: str, notes: str = ""):
        """Update brand_data_requests table"""

        if not self.sb_url or not self.sb_key:
            return

        try:
            sb = create_client(self.sb_url, self.sb_key)
            sb.table("brand_data_requests").update(
                {
                    "status": status,
                    "research_notes": notes,
                    "completed_at": datetime.now().isoformat(),
                }
            ).eq("brand_name", brand_name).eq("category", category if category != "unknown" else None).execute()
        except Exception as e:
            print(f"Update request error: {e}")

    def send_notification_email(self, email: str, brand_name: str):
        """Send email notification that brand has been added"""

        if not email:
            return

        try:
            # Using Gmail or configured email service
            sender_email = os.environ.get("SENDER_EMAIL")
            sender_password = os.environ.get("SENDER_PASSWORD")

            if not sender_email or not sender_password:
                print("Email credentials not configured - skipping notification")
                return

            subject = f"✅ {brand_name} Added to Intel!"
            body = f"""Hi,

Great news! {brand_name} has been researched and added to Intel Phase 1.

You can now:
- Search for {brand_name} at intel.humanagency.co/brand
- Compare with competitors
- View market data and insights

Start exploring: https://intel.humanagency.co/brand

Thanks for helping us build a more complete Intel database!

—The Miru Team"""

            message = MIMEMultipart()
            message["From"] = sender_email
            message["To"] = email
            message["Subject"] = subject
            message.attach(MIMEText(body, "plain"))

            # Send via SMTP
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(sender_email, sender_password)
                server.send_message(message)

            print(f"Email sent to {email}")
        except Exception as e:
            print(f"Email error: {e}")

    def process_request(self, brand_name: str, category_hint: str = "", email: str = ""):
        """
        Process a brand request end-to-end:
        1. Research brand
        2. Add to database (UK, USA, India)
        3. Update request status
        4. Send email
        """

        print(f"\n🤖 [AGENT] Starting process_request: {brand_name} (category={category_hint}, email={email})")

        # 1. Research
        print(f"[AGENT] Researching {brand_name}...")
        brand_data = self.research_brand(brand_name, category_hint)

        if not brand_data:
            print(f"[AGENT] Failed to research {brand_name}")
            self.update_request_status(brand_name, category_hint, "failed", "LLM research failed")
            return False

        print(f"[AGENT] Research complete: {brand_data.get('category')} brand")

        # 2. Add to database
        print(f"[AGENT] Adding {brand_name} to database...")
        markets = ["UK", "USA", "India"]
        success = self.add_brand_to_database(brand_data, markets)

        if not success:
            print(f"[AGENT] Failed to add {brand_name} to database")
            self.update_request_status(brand_name, category_hint, "failed", "Database insert failed")
            return False

        print(f"[AGENT] {brand_name} added across {', '.join(markets)}")

        # 3. Update request status
        final_category = brand_data.get("category", category_hint)
        self.update_request_status(brand_name, category_hint, "collected", f"Added across {', '.join(markets)}")

        # 4. Send email notification
        if email:
            print(f"[AGENT] Sending notification to {email}...")
            self.send_notification_email(email, brand_name)

        print(f"[AGENT] ✅ {brand_name} research complete!\n")
        return True


# CLI for testing
if __name__ == "__main__":
    agent = BrandResearchAgent()

    # Test with Nutella
    result = agent.process_request(
        brand_name="Nutella",
        category_hint="snacks",
        email="mekala@gmail.com"
    )

    print(f"Result: {result}")

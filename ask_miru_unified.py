"""
Unified Ask Miru — Query all Miru data sources
Spending, school, calendar, patterns, habits
"""
import json

class AskMiruUnified:
    """Query engine for all Miru data"""
    
    def __init__(self, from_number, sb):
        self.from_number = from_number
        self.sb = sb
        self.phone = from_number.replace("whatsapp:", "").strip()
    
    def query(self, question: str) -> str:
        """Answer question using all Miru data"""
        q = question.lower()

        # Route to specific handlers (check receipts FIRST for "when did I have")
        if any(w in q for w in ["receipt", "bought", "shop", "visit", "had", "croissant", "coffee", "almond", "bento"]):
            return self.query_receipts(question)
        elif any(w in q for w in ["spend", "cost", "how much", "budget", "expensive", "money", "afford"]):
            return self.query_spending(question)
        elif any(w in q for w in ["school", "event", "activity", "appointment", "riaan", "inaaya"]):
            return self.query_school_calendar(question)
        elif any(w in q for w in ["restaurant", "favorite", "usual", "habit", "routine", "always", "often"]):
            return self.query_patterns(question)
        else:
            return "I can help with: spending, school events, favorite places, shopping habits, and recent purchases."
    
    def query_spending(self, question: str) -> str:
        """Query spending by category"""
        try:
            rows = self.sb.table("wa_saves").select("summary,created_at") \
                .eq("from_number", self.phone) \
                .ilike("category", "%Dining%").or_("category", "ilike", "%Coffee%") \
                .order("created_at", desc=True).limit(20).execute().data or []
            
            total = 0
            for r in rows:
                summary = r.get("summary", "")
                import re
                match = re.search(r'£([\d,]+\.?\d{0,2})', summary)
                if match:
                    total += float(match.group(1).replace(",", ""))
            
            if total > 0:
                return f"You've spent £{total:.2f} on food and coffee this week."
            else:
                return "No recent spending recorded."
        except Exception as e:
            return f"Couldn't check spending: {e}"
    
    def query_school_calendar(self, question: str) -> str:
        """Query school events and calendar"""
        try:
            from datetime import datetime, timedelta
            today = datetime.now().date()

            # Try to query school_events table with safe column names
            rows = self.sb.table("school_events").select("*") \
                .gte("event_date", today.isoformat()) \
                .order("event_date", desc=False).limit(10).execute().data or []

            if rows:
                next_event = rows[0]
                # Handle different column name variations
                child = next_event.get("child_name") or next_event.get("child") or ""
                event = next_event.get("event_title") or next_event.get("title") or ""
                date_str = next_event.get("event_date") or next_event.get("date") or ""
                if child and event:
                    return f"Next: {child}'s {event} on {date_str}"
                else:
                    return "No upcoming school events."
            else:
                return "No upcoming school events."
        except Exception as e:
            return f"No school events found."
    
    def query_patterns(self, question: str) -> str:
        """Query habits and patterns from receipts"""
        try:
            rows = self.sb.table("receipts").select("merchant,items,shop_date") \
                .eq("phone", self.phone).order("shop_date", desc=True).limit(20).execute().data or []
            
            merchants = {}
            for r in rows:
                merchant = r.get("merchant", "").lower()
                if merchant:
                    merchants[merchant] = merchants.get(merchant, 0) + 1
            
            if merchants:
                top = sorted(merchants.items(), key=lambda x: x[1], reverse=True)[0]
                return f"You visit {top[0].title()} most often ({top[1]} times recently)."
            else:
                return "No receipt history found."
        except Exception as e:
            return f"Couldn't check patterns: {e}"
    
    def query_receipts(self, question: str) -> str:
        """Query recent receipts and purchases (from wa_saves 🧾 receipts first, then receipts table)"""
        try:
            import json as _json
            import re as _re

            # Search for specific item if mentioned (any word except stop words)
            stop_words = {"when", "did", "i", "have", "had", "a", "the", "at", "in", "on", "is", "was", "were", "be", "been", "with", "from", "to", "and", "or", "not", "no", "yes", "do", "you", "me", "my", "this", "that", "what", "where", "how", "why", "receipt", "shop", "visit", "bought"}
            search_item = None
            for word in question.lower().split():
                # Clean punctuation
                word = word.strip('?,.:;!"\'-')
                if word and len(word) > 2 and word not in stop_words:
                    search_item = word
                    break

            if search_item:
                # FIRST: Search wa_saves 🧾 receipts (newest first)
                wa_receipts = self.sb.table("wa_saves").select("title,summary,created_at") \
                    .eq("from_number", self.phone).ilike("title", "%🧾%") \
                    .order("created_at", desc=True).limit(50).execute().data or []

                # Check if user mentioned a merchant name (keywords like "at", "in", "from")
                q_lower = question.lower()
                merchant_search = None
                for m in ["tesco", "sainsbury", "waitrose", "asda", "costa", "pret", "indian cart",
                          "kokoro", "chaiiwala", "greggs", "mcdonald", "subway", "pizza"]:
                    if m in q_lower:
                        merchant_search = m
                        break

                # Search strategy 1: If merchant mentioned, find MOST RECENT receipt from that merchant
                if merchant_search:
                    for r in wa_receipts:
                        merchant = r.get("title", "").replace("🧾", "").strip().lower()
                        if merchant_search.lower() in merchant:
                            date_str = r.get("created_at", "")[:10]
                            # Extract items from summary
                            summary = r.get("summary", "")
                            items = summary.split('\n')[0] if summary else ""  # First line usually has items
                            amt_match = _re.search(r'£([\d,]+\.?\d*)', summary)
                            amt = f" (£{amt_match.group(1)})" if amt_match else ""
                            return f"You bought {items} from {r.get('title', '').replace('🧾', '').strip()} on {date_str}{amt}"

                # Search strategy 2: Look for item in summary (generic search)
                for r in wa_receipts:
                    summary = r.get("summary", "").lower()
                    if search_item.lower() in summary:
                        merchant = r.get("title", "").replace("🧾", "").strip()
                        date_str = r.get("created_at", "")[:10]
                        # Extract amount if present
                        amt_match = _re.search(r'£([\d,]+\.?\d*)', r.get("summary", ""))
                        amt = f" (£{amt_match.group(1)})" if amt_match else ""
                        return f"You had {search_item} at {merchant} on {date_str}{amt}"

                # FALLBACK: Search receipts table if wa_saves had nothing
                rows = self.sb.table("receipts").select("merchant,items,shop_date,total") \
                    .eq("phone", self.phone).order("shop_date", desc=True).limit(50).execute().data or []

                for r in rows:
                    items_json = r.get("items", "[]")
                    try:
                        items_list = _json.loads(items_json) if isinstance(items_json, str) else (items_json or [])
                    except:
                        items_list = []

                    for item in items_list:
                        # Handle both dict format {"name": "..."} and string format "..."
                        if isinstance(item, dict):
                            item_name = item.get("name", "")
                        else:
                            item_name = str(item).strip()

                        if item_name and search_item.lower() in item_name.lower():
                            merchant = r.get("merchant", "")
                            date_str = r.get("shop_date", "")
                            return f"You had {item_name} at {merchant} on {date_str}"

                return f"I didn't find {search_item} in your recent receipts."
            else:
                # Just show last receipt from wa_saves
                wa_receipts = self.sb.table("wa_saves").select("title,summary,created_at") \
                    .eq("from_number", self.phone).ilike("title", "%🧾%") \
                    .order("created_at", desc=True).limit(1).execute().data or []

                if wa_receipts:
                    r = wa_receipts[0]
                    merchant = r.get("title", "").replace("🧾", "").strip()
                    date_str = r.get("created_at", "")[:10]
                    return f"Last receipt: {merchant} on {date_str}"

                # Fallback to receipts table
                rows = self.sb.table("receipts").select("merchant,shop_date") \
                    .eq("phone", self.phone).order("shop_date", desc=True).limit(1).execute().data or []

                if rows:
                    r = rows[0]
                    merchant = r.get("merchant", "")
                    date_str = r.get("shop_date", "")
                    return f"Last visit: {merchant} on {date_str}"
                else:
                    return "No recent receipts."
        except Exception as e:
            return f"Couldn't check receipts: {e}"

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
        
        # Route to specific handlers
        if any(w in q for w in ["spend", "cost", "how much", "budget", "expensive", "money", "afford"]):
            return self.query_spending(question)
        elif any(w in q for w in ["school", "event", "activity", "when", "appointment", "riaan", "inaaya"]):
            return self.query_school_calendar(question)
        elif any(w in q for w in ["restaurant", "coffee", "favorite", "usual", "habit", "routine", "always", "often"]):
            return self.query_patterns(question)
        elif any(w in q for w in ["receipt", "bought", "shop", "visit", "last", "previous"]):
            return self.query_receipts(question)
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
            
            rows = self.sb.table("school_events").select("event_title,event_date,child_name") \
                .gte("event_date", today.isoformat()) \
                .order("event_date", desc=False).limit(10).execute().data or []
            
            if rows:
                next_event = rows[0]
                child = next_event.get("child_name", "")
                event = next_event.get("event_title", "")
                date_str = next_event.get("event_date", "")
                return f"Next: {child}'s {event} on {date_str}"
            else:
                return "No upcoming school events."
        except Exception as e:
            return f"Couldn't check events: {e}"
    
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
        """Query recent receipts and purchases"""
        try:
            import json as _json
            rows = self.sb.table("receipts").select("merchant,items,shop_date,total") \
                .eq("phone", self.phone).order("shop_date", desc=True).limit(50).execute().data or []

            # Search for specific item if mentioned
            search_item = None
            for word in question.lower().split():
                if word in ["croissant", "chocolate", "pain", "almond", "coffee", "tea", "bento", "udon"]:
                    search_item = word
                    break

            if search_item:
                # Find receipt with this item
                for r in rows:
                    items_json = r.get("items", "[]")
                    try:
                        items_list = _json.loads(items_json) if isinstance(items_json, str) else (items_json or [])
                    except:
                        items_list = []

                    for item in items_list:
                        item_name = item.get("name", "") if isinstance(item, dict) else str(item)
                        if search_item.lower() in item_name.lower():
                            merchant = r.get("merchant", "")
                            date_str = r.get("shop_date", "")
                            return f"You had {item_name} at {merchant} on {date_str}"

                return f"I didn't find {search_item} in your recent receipts."
            else:
                # Just show last receipt
                if rows:
                    r = rows[0]
                    merchant = r.get("merchant", "")
                    date_str = r.get("shop_date", "")
                    return f"Last visit: {merchant} on {date_str}"
                else:
                    return "No recent receipts."
        except Exception as e:
            return f"Couldn't check receipts: {e}"

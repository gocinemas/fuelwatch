"""
Ask Miru RAG System — Smart retrieval + inference for personal data queries
- Entity extraction (merchant, date, item, amount)
- Unified database retrieval (wa_saves + receipts table)
- Claude-powered synthesis (data-first, no hallucination)
- Context memory for follow-ups
"""
import json
import re
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Any


class EntityExtractor:
    """Extract structured data from natural language queries"""

    # Known merchants and their aliases
    MERCHANTS = {
        "indian cart": ["indian cart", "the indian cart", "indian cart ltd"],
        "pret": ["pret", "pret a manger", "pret manger"],
        "tesco": ["tesco", "tesco supermarket"],
        "sainsbury": ["sainsbury", "sainsburys", "sainsbury's"],
        "waitrose": ["waitrose"],
        "costa": ["costa", "costa coffee"],
        "mcdonald": ["mcdonald", "mcdonalds", "mcd", "maccas"],
        "subway": ["subway"],
        "greggs": ["greggs"],
        "asda": ["asda"],
        "kokoro": ["kokoro"],
        "chaiiwala": ["chaiiwala", "chai wala"],
    }

    # Time qualifiers
    TIME_QUALIFIERS = {
        "today": ["today", "this morning", "this afternoon", "this evening", "right now"],
        "yesterday": ["yesterday"],
        "this week": ["this week", "past week", "last week", "this past week"],
        "this month": ["this month", "past month", "last month"],
        "recent": ["recent", "lately", "last time", "previously"],
    }

    @staticmethod
    def extract_merchant(query: str) -> Optional[str]:
        """Extract merchant name from query"""
        q_lower = query.lower()
        for canonical, aliases in EntityExtractor.MERCHANTS.items():
            for alias in aliases:
                if alias in q_lower:
                    return canonical
        return None

    @staticmethod
    def extract_time_qualifier(query: str) -> Optional[str]:
        """Extract time context (today, yesterday, this week, etc.)"""
        q_lower = query.lower()
        for time_qual, triggers in EntityExtractor.TIME_QUALIFIERS.items():
            for trigger in triggers:
                if trigger in q_lower:
                    return time_qual
        return None

    @staticmethod
    def extract_item(query: str) -> Optional[str]:
        """Extract specific item name from query"""
        stop_words = {
            "did", "i", "you", "have", "had", "buy", "get", "order", "eat", "drink",
            "at", "in", "from", "the", "a", "an", "what", "when", "where", "how",
            "my", "me", "today", "yesterday", "last", "time", "visit", "shop", "receipt",
            "items", "things", "stuff", "indian", "cart", "pret", "tesco", "this", "that",
            "your", "mine", "ours", "week", "month", "days"
        }

        words = re.findall(r'\b\w+\b', query.lower())
        for word in words:
            if len(word) > 3 and word not in stop_words:
                return word
        return None

    @staticmethod
    def extract_amount(query: str) -> Optional[float]:
        """Extract monetary amount from query"""
        match = re.search(r'£([\d,]+\.?\d{0,2})', query)
        if match:
            return float(match.group(1).replace(",", ""))
        return None


class MiruRAG:
    """Unified RAG system for personal data retrieval"""

    def __init__(self, phone: str, sb):
        # Keep original for exact matching (database stores "whatsapp:+44..." format)
        self.phone_original = phone

        # Also normalize: remove "whatsapp:" prefix, handle +/no-+ formats
        self.phone_raw = phone.replace("whatsapp:", "").strip()
        self.phone = self.phone_raw.lstrip("+")  # Remove leading + if present
        self.phone_with_plus = f"+{self.phone}" if not self.phone.startswith("+") else self.phone

        self.sb = sb
        self.context_history: List[Dict] = []  # Track query context for follow-ups

    def query(self, question: str) -> Dict[str, Any]:
        """
        Main query interface. Returns structured result with data + metadata.

        Returns:
            {
                "answer": "User-facing response",
                "data": {...},  # Raw retrieved data
                "source": "wa_saves|receipts|groq",
                "confidence": 0.0-1.0,
                "context": {...}  # For follow-ups
            }
        """
        q_lower = question.lower()

        # Extract entities
        merchant = EntityExtractor.extract_merchant(question)
        time_qual = EntityExtractor.extract_time_qualifier(question)
        item = EntityExtractor.extract_item(question)

        # Route to appropriate handler
        if any(w in q_lower for w in ["what", "did i", "did you", "have i", "what items"]):
            # Question about purchases/items
            return self._query_receipts(merchant, item, time_qual, question)
        elif any(w in q_lower for w in ["how much", "spent", "cost", "budget"]):
            return self._query_spending(merchant, time_qual, question)
        elif any(w in q_lower for w in ["when", "date", "time"]):
            return self._query_dates(merchant, time_qual, question)
        else:
            return {
                "answer": "I can help with: spending, receipts, items you bought, and patterns.",
                "source": "system",
                "confidence": 1.0,
            }

    def _query_receipts(
        self, merchant: Optional[str], item: Optional[str], time_qual: Optional[str], question: str
    ) -> Dict[str, Any]:
        """Query receipts from wa_saves and receipts table"""
        try:
            # 1. TRY wa_saves FIRST (most reliable, has real receipt data)
            wa_result = self._query_wa_saves(merchant, item, time_qual)
            if wa_result.get("found"):
                self.context_history.append({"type": "receipt", "merchant": merchant, "items": wa_result.get("items")})
                return wa_result

            # 2. FALLBACK to receipts table
            rcpt_result = self._query_receipts_table(merchant, item, time_qual)
            if rcpt_result.get("found"):
                self.context_history.append({"type": "receipt", "merchant": merchant, "items": rcpt_result.get("items")})
                return rcpt_result

            # 3. NO DATA FOUND - return clear message, no hallucination
            return {
                "answer": f"I didn't find receipt data for {merchant or 'this merchant'}.",
                "data": None,
                "source": "database",
                "confidence": 1.0,
                "found": False,
            }

        except Exception as e:
            return {"answer": f"Error querying receipts: {e}", "source": "error", "confidence": 0.0}

    def _query_wa_saves(self, merchant: Optional[str], item: Optional[str], time_qual: Optional[str]) -> Dict:
        """Query wa_saves table (🧾 receipts)"""
        try:
            # Try multiple phone formats (database stores "whatsapp:+44..." format)
            phone_formats = [
                self.phone_original,  # Original: "whatsapp:+447595075735"
                self.phone,  # Plain: "447595075735"
                self.phone_with_plus,  # With +: "+447595075735"
                self.phone_raw,  # Raw: "+447595075735" or "whatsapp:+447595075735"
            ]
            print(f"[RAG DEBUG] Querying wa_saves for phone formats: {phone_formats}")

            rows = self.sb.table("wa_saves").select("title,summary,created_at").in_(
                "from_number", phone_formats
            ).ilike("title", "%🧾%")

            query = rows

            # Filter by merchant if specified
            if merchant:
                print(f"[RAG DEBUG] Filtering by merchant: {merchant}")
                query = query.ilike("title", f"%{merchant}%")

            # Filter by time
            if time_qual == "today":
                today = date.today().isoformat()
                query = query.gte("created_at", f"{today}T00:00:00").lte("created_at", f"{today}T23:59:59")
            elif time_qual == "yesterday":
                yesterday = (date.today() - timedelta(days=1)).isoformat()
                query = query.gte("created_at", f"{yesterday}T00:00:00").lte("created_at", f"{yesterday}T23:59:59")

            rows = query.order("created_at", desc=True).limit(5).execute().data or []

            print(f"[RAG DEBUG] Found {len(rows)} rows in wa_saves")
            if rows:
                for i, r in enumerate(rows[:3]):
                    print(f"  Row {i}: title={r.get('title')}, created_at={r.get('created_at')}")

            if not rows:
                print(f"[RAG DEBUG] No rows found, returning not found")
                return {"found": False, "data": None}

            # Process results
            receipt = rows[0]  # Most recent
            merchant_name = receipt.get("title", "").replace("🧾", "").strip()
            summary = receipt.get("summary", "")
            created_at = receipt.get("created_at", "")[:10]

            # Extract items from summary
            items = self._parse_receipt_items(summary)

            # Extract amount
            amount_match = re.search(r"£([\d,]+\.?\d{0,2})", summary)
            amount = f"£{amount_match.group(1)}" if amount_match else None

            answer = f"You ordered from {merchant_name} on {created_at}"
            if items:
                answer += f":\n\n" + "\n".join(items[:12])
            if amount:
                answer += f"\n\nTotal: {amount}"

            return {
                "answer": answer,
                "data": {
                    "merchant": merchant_name,
                    "date": created_at,
                    "items": items,
                    "amount": amount,
                    "summary": summary,
                },
                "source": "wa_saves",
                "confidence": 0.95,
                "found": True,
            }

        except Exception as e:
            return {"found": False, "error": str(e)}

    def _query_receipts_table(self, merchant: Optional[str], item: Optional[str], time_qual: Optional[str]) -> Dict:
        """Query receipts table (PDF imports, structured data)"""
        try:
            # Try multiple phone formats
            phone_formats = [
                self.phone_original.replace("whatsapp:", "").strip(),  # Remove whatsapp prefix
                self.phone,
                self.phone_with_plus,
                self.phone_raw,
            ]
            query = self.sb.table("receipts").select("merchant,items,shop_date,total,created_at").in_(
                "phone", phone_formats
            )

            if merchant:
                query = query.ilike("merchant", f"%{merchant}%")

            if time_qual == "today":
                today = date.today().isoformat()
                query = query.gte("shop_date", today).lt("shop_date", (date.today() + timedelta(days=1)).isoformat())

            rows = query.order("shop_date", desc=True).limit(5).execute().data or []

            if not rows:
                return {"found": False}

            receipt = rows[0]
            merchant_name = receipt.get("merchant", "")
            shop_date = receipt.get("shop_date", "")[:10]

            # Parse items
            items_json = receipt.get("items", "[]")
            try:
                items_list = json.loads(items_json) if isinstance(items_json, str) else (items_json or [])
            except:
                items_list = []

            items = []
            for it in items_list:
                if isinstance(it, dict):
                    name = it.get("name", "").strip()
                else:
                    name = str(it).strip()
                if name:
                    items.append(name)

            amount = receipt.get("total")
            amount_str = f"£{amount:.2f}" if amount else None

            answer = f"You ordered from {merchant_name} on {shop_date}"
            if items:
                answer += ":\n\n" + "\n".join(items[:12])
            if amount_str:
                answer += f"\n\nTotal: {amount_str}"

            return {
                "answer": answer,
                "data": {
                    "merchant": merchant_name,
                    "date": shop_date,
                    "items": items,
                    "amount": amount_str,
                },
                "source": "receipts",
                "confidence": 0.9,
                "found": True,
            }

        except Exception as e:
            return {"found": False, "error": str(e)}

    def _query_spending(self, merchant: Optional[str], time_qual: Optional[str], question: str) -> Dict:
        """Query spending by merchant or time period"""
        try:
            # Get recent purchases and sum by merchant
            phone_formats = [
                self.phone_original,  # Original format from database
                self.phone,
                self.phone_with_plus,
                self.phone_raw,
            ]
            rows = self.sb.table("wa_saves").select("title,summary,created_at").in_(
                "from_number", phone_formats
            ).ilike("title", "%🧾%").order("created_at", desc=True).limit(50).execute().data or []

            if not rows:
                return {"answer": "No spending data found.", "source": "database", "confidence": 1.0}

            spending = {}
            for r in rows:
                merchant_name = r.get("title", "").replace("🧾", "").strip()
                summary = r.get("summary", "")

                # Extract amount
                match = re.search(r"£([\d,]+\.?\d{0,2})", summary)
                if match:
                    amount = float(match.group(1).replace(",", ""))
                    if merchant_name not in spending:
                        spending[merchant_name] = 0
                    spending[merchant_name] += amount

            if not spending:
                return {"answer": "No spending data found.", "source": "database", "confidence": 1.0}

            # Format answer
            sorted_spending = sorted(spending.items(), key=lambda x: x[1], reverse=True)
            total = sum(a for _, a in sorted_spending)

            lines = [f"Total spending: £{total:.2f}\n"]
            for merchant_name, amount in sorted_spending[:5]:
                lines.append(f"  {merchant_name}: £{amount:.2f}")

            return {
                "answer": "\n".join(lines),
                "data": {"spending": dict(sorted_spending), "total": total},
                "source": "wa_saves",
                "confidence": 0.95,
            }

        except Exception as e:
            return {"answer": f"Error querying spending: {e}", "source": "error", "confidence": 0.0}

    def _query_dates(self, merchant: Optional[str], time_qual: Optional[str], question: str) -> Dict:
        """Query when receipts were from"""
        # Delegate to receipt query to get the date
        result = self._query_receipts(merchant, None, time_qual, question)
        if result.get("found"):
            date_str = result.get("data", {}).get("date")
            answer = f"That was on {date_str}."
            return {
                "answer": answer,
                "data": result.get("data"),
                "source": result.get("source"),
                "confidence": result.get("confidence"),
            }
        return result

    @staticmethod
    def _parse_receipt_items(summary: str) -> List[str]:
        """Extract item list from receipt summary"""
        if not summary:
            return []

        lines = summary.split("\n")
        items = []

        for line in lines:
            line = line.strip()
            # Skip empty lines and lines that are just totals/prices
            if not line or line.startswith("Total") or line.startswith("TOTAL"):
                continue
            # Include lines that look like items (contain product names, prices optional)
            if len(line) > 3 and not line.startswith("---"):
                items.append(line)

        return items[:20]  # Limit to 20 items

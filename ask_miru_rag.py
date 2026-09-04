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
        """
        Initialize RAG with phone number in any format:
        - "whatsapp:+447595075735"
        - "whatsapp:447595075735"
        - "+447595075735"
        - "447595075735"

        Generates all variants for flexible database queries (different users
        may have phone stored in different formats).
        """
        self.phone_original = phone
        self.sb = sb
        self.context_history: List[Dict] = []

        # Generate all possible phone format variants for database queries
        self._generate_phone_variants()

    def _generate_phone_variants(self):
        """Generate all possible phone number formats for flexible database matching"""
        variants = set()

        # Start with original
        variants.add(self.phone_original)

        # Remove whatsapp prefix
        no_wa = self.phone_original.replace("whatsapp:", "").strip()
        variants.add(no_wa)

        # Handle + prefix variations
        if no_wa.startswith("+"):
            variants.add(no_wa)  # With +
            variants.add(no_wa[1:])  # Without +
        else:
            variants.add(no_wa)  # Without +
            if no_wa:  # Only add + version if phone is non-empty
                variants.add(f"+{no_wa}")  # With +

        # Also try with whatsapp: prefix + variations
        no_plus = no_wa.lstrip("+")
        if no_plus:
            variants.add(f"whatsapp:{no_plus}")
            variants.add(f"whatsapp:+{no_plus}")

        # Remove any empty strings and duplicates
        self.phone_variants = sorted(list(set(v for v in variants if v and v.strip())))

        # Debug logging
        if len(self.phone_variants) == 0:
            # Fallback: at least include the original
            self.phone_variants = [self.phone_original] if self.phone_original else []

    def query(self, question: str) -> Dict[str, Any]:
        """
        Main query interface. Returns structured result with data + metadata.

        ALWAYS returns a valid response dict with "answer" field, never throws.

        Returns:
            {
                "answer": "User-facing response",
                "data": {...},  # Raw retrieved data
                "source": "wa_saves|receipts|groq",
                "confidence": 0.0-1.0,
                "context": {...}  # For follow-ups
            }
        """
        try:
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
            elif merchant:
                # If a merchant name is mentioned alone (e.g., "kokoro"), treat as receipt query
                return self._query_receipts(merchant, None, None, question)
            else:
                return {
                    "answer": "I can help with: spending, receipts, items you bought, and patterns.",
                    "source": "system",
                    "confidence": 1.0,
                }
        except Exception as e:
            # Emergency fallback: ALWAYS return something with an answer
            return {
                "answer": f"Sorry, I encountered an error processing that query: {str(e)[:50]}",
                "source": "error",
                "confidence": 0.0,
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

            # If wa_saves explicitly searched for merchant and found nothing, don't fall back
            if merchant and wa_result.get("reason"):
                return {
                    "answer": f"I didn't find {merchant} in your receipts.",
                    "data": None,
                    "source": "database",
                    "confidence": 1.0,
                    "found": False,
                }

            # 2. FALLBACK to receipts table
            rcpt_result = self._query_receipts_table(merchant, item, time_qual)
            if rcpt_result.get("found"):
                self.context_history.append({"type": "receipt", "merchant": merchant, "items": rcpt_result.get("items")})
                return rcpt_result

            # 3. NO DATA FOUND - return clear message, no hallucination
            if merchant:
                msg = f"I didn't find {merchant} in your receipts."
            else:
                msg = "I didn't find receipt data for that query."

            return {
                "answer": msg,
                "data": None,
                "source": "database",
                "confidence": 1.0,
                "found": False,
            }

        except Exception as e:
            return {"answer": f"Error querying receipts: {e}", "source": "error", "confidence": 0.0}

    def _query_wa_saves(self, merchant: Optional[str], item: Optional[str], time_qual: Optional[str]) -> Dict:
        """Query wa_saves table (🧾 receipts) — works with all phone formats"""
        try:
            rows = self.sb.table("wa_saves").select("title,summary,created_at").in_(
                "from_number", self.phone_variants
            ).ilike("title", "%🧾%")

            query = rows

            # Filter by merchant if specified (STRICT matching when merchant is requested)
            if merchant:
                # Case-insensitive search: title ILIKE '%{merchant}%'
                query = query.ilike("title", f"%{merchant}%")
                # Store flag: if merchant was explicitly requested, we should NOT return other merchants
                requested_merchant = merchant.lower()
            else:
                requested_merchant = None

            # Filter by time
            if time_qual == "today":
                today = date.today().isoformat()
                query = query.gte("created_at", f"{today}T00:00:00").lte("created_at", f"{today}T23:59:59")
            elif time_qual == "yesterday":
                yesterday = (date.today() - timedelta(days=1)).isoformat()
                query = query.gte("created_at", f"{yesterday}T00:00:00").lte("created_at", f"{yesterday}T23:59:59")

            rows = query.order("created_at", desc=True).limit(50).execute().data or []  # Fetch more rows to filter by item

            if not rows:
                # If merchant was explicitly requested and not found, return clear "not found"
                if requested_merchant:
                    return {
                        "found": False,
                        "data": None,
                        "reason": f"No receipts found for {requested_merchant}"
                    }
                return {"found": False, "data": None}

            # CRITICAL: If item is specified, filter receipts by item content
            if item:
                item_lower = item.lower()
                item_words = [w.lower().strip() for w in item_lower.split() if w.strip() and len(w.strip()) > 1]

                matching_receipts = []
                for receipt in rows:
                    summary = receipt.get("summary", "").lower()
                    title = receipt.get("title", "").lower()
                    # ALL search words must match in title or summary
                    if all(word in (title + " " + summary) for word in item_words):
                        matching_receipts.append(receipt)

                # If no receipts match the item, return "not found"
                if not matching_receipts:
                    return {
                        "answer": f"🔍 No receipts found with '{item}'.",
                        "data": None,
                        "source": "database",
                        "confidence": 1.0,
                        "found": False,
                    }

                rows = matching_receipts

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

            # Format cleanly: merchant + date, then items
            items_text = "\n".join([f"  • {item}" for item in items[:12]]) if items else ""
            answer = f"🧾 {merchant_name} on {created_at}"
            if items_text:
                answer += f"\n{items_text}"
            if amount:
                answer += f"\n💰 {amount}"

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
        """Query receipts table (PDF imports, structured data) — works with all phone formats"""
        try:
            # Remove whatsapp: prefix for receipts table (uses plain phone only)
            phone_variants_plain = [v.replace("whatsapp:", "").strip() for v in self.phone_variants]
            phone_variants_plain = [v for v in phone_variants_plain if v]  # Remove empty

            query = self.sb.table("receipts").select("merchant,items,shop_date,total,created_at").in_(
                "phone", phone_variants_plain
            )

            # Store if merchant was explicitly requested (for error messaging)
            requested_merchant = merchant.lower() if merchant else None

            if merchant:
                query = query.ilike("merchant", f"%{merchant}%")

            if time_qual == "today":
                today = date.today().isoformat()
                query = query.gte("shop_date", today).lt("shop_date", (date.today() + timedelta(days=1)).isoformat())

            rows = query.order("shop_date", desc=True).limit(200).execute().data or []  # Fetch more to filter by item

            if not rows:
                # If merchant was explicitly requested and not found, return clear "not found"
                if requested_merchant:
                    return {
                        "found": False,
                        "reason": f"No receipts found for {requested_merchant}"
                    }
                return {"found": False}

            # CRITICAL: If item is specified, filter receipts by item content
            if item:
                item_lower = item.lower()
                item_words = [w.lower().strip() for w in item_lower.split() if w.strip() and len(w.strip()) > 1]

                matching_receipts = []
                for receipt in rows:
                    try:
                        items_json = receipt.get("items", "[]")
                        items_list = json.loads(items_json) if isinstance(items_json, str) else (items_json or [])
                    except:
                        items_list = []

                    # Check if ANY item in the receipt matches ALL search words
                    for it in items_list:
                        item_name = it.get("name", "") if isinstance(it, dict) else str(it)
                        item_name_lower = item_name.lower()

                        # ALL search words must match this item
                        if all(word in item_name_lower for word in item_words):
                            matching_receipts.append(receipt)
                            break  # Found a match for this receipt, move to next

                # If no receipts match the item, return "not found"
                if not matching_receipts:
                    return {
                        "answer": f"🔍 No receipts found with '{item}'.",
                        "data": None,
                        "source": "database",
                        "confidence": 1.0,
                        "found": False,
                    }

                rows = matching_receipts

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

            # Format cleanly: merchant + date, then items
            items_text = "\n".join([f"  • {item}" for item in items[:12]]) if items else ""
            answer = f"🧾 {merchant_name} on {shop_date}"
            if items_text:
                answer += f"\n{items_text}"
            if amount_str:
                answer += f"\n💰 {amount_str}"

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
        """Query spending by merchant or time period — works with all phone formats"""
        try:
            # Get recent purchases and sum by merchant
            rows = self.sb.table("wa_saves").select("title,summary,created_at").in_(
                "from_number", self.phone_variants
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

"""
Ask Miru RAG System — Smart retrieval + inference for personal data queries
- Entity extraction (merchant, date, item, amount)
- Algolia search (fast full-text index) + Database retrieval (wa_saves + receipts table)
- Claude-powered synthesis (data-first, no hallucination)
- Context memory for follow-ups
"""
import json
import re
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Any

try:
    from algolia_service import get_algolia
except ImportError:
    get_algolia = lambda: None


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
        print(f"[DEBUG _generate_phone_variants] phone_original={self.phone_original}", flush=True)
        variants = set()

        # Start with original
        variants.add(self.phone_original)

        # Remove whatsapp prefix
        no_wa = self.phone_original.replace("whatsapp:", "").strip()
        variants.add(no_wa)

        # Handle + prefix variations
        if no_wa.startswith("+"):
            variants.add(no_wa)  # +447595075735
            no_plus = no_wa[1:]
            variants.add(no_plus)  # 447595075735
            variants.add(f"00{no_plus}")  # 00447595075735
            # UK mobile format: +447595075735 → 07595075735
            if no_plus.startswith("447"):
                variants.add("0" + no_plus[2:])  # 07595075735
        else:
            variants.add(no_wa)  # As-is
            if no_wa:
                variants.add(f"+{no_wa}")  # With +
                # Add UK formats
                if no_wa.startswith("447"):
                    variants.add(f"00{no_wa}")  # 00447...
                    variants.add("0" + no_wa[2:])  # 07...

        # Also try with whatsapp: prefix + variations
        no_plus = no_wa.lstrip("+")
        if no_plus:
            variants.add(f"whatsapp:{no_plus}")
            variants.add(f"whatsapp:+{no_plus}")
            # whatsapp: + UK formats
            if no_plus.startswith("447"):
                variants.add(f"whatsapp:00{no_plus}")
                variants.add(f"whatsapp:0{no_plus[2:]}")

        # Remove any empty strings and duplicates
        self.phone_variants = sorted(list(set(v for v in variants if v and v.strip())))

        # Debug logging
        if len(self.phone_variants) == 0:
            # Fallback: at least include the original
            self.phone_variants = [self.phone_original] if self.phone_original else []

        print(f"[DEBUG _generate_phone_variants] Generated variants: {self.phone_variants}", flush=True)

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

            print(f"[DEBUG query] question='{question}' → merchant={merchant}, item={item}, time_qual={time_qual}", flush=True)

            # Check if user is asking about a product type (wines, beers, etc.)
            product_types = ["wine", "wines", "beer", "beers", "coffee", "tea", "chocolate", "book", "books"]
            is_product_query = any(pt in q_lower for pt in product_types)

            # Route to appropriate handler
            if is_product_query:
                # Search BOTH receipts and saved items for products
                print(f"[DEBUG] Product type query detected: {question}", flush=True)
                print(f"[DEBUG] Calling _query_receipts with merchant={merchant}, item={item}", flush=True)
                receipt_result = self._query_receipts(merchant, item, time_qual, question)
                print(f"[DEBUG] Receipt result: found={receipt_result.get('found')}, answer={receipt_result.get('answer', '')[:80]}", flush=True)
                saved_result = self._query_saved_links(question)
                print(f"[DEBUG] Saved result: found={saved_result.get('found')}, answer={saved_result.get('answer', '')[:80]}", flush=True)

                # Combine results from both sources
                combined_answer = ""
                if receipt_result.get("found"):
                    combined_answer += f"📦 From receipts:\n{receipt_result.get('answer', '')}\n\n"
                if saved_result.get("found"):
                    combined_answer += f"💾 From saved items:\n{saved_result.get('answer', '')}\n"

                if combined_answer.strip():
                    return {
                        "found": True,
                        "answer": combined_answer.strip(),
                        "source": "combined",
                        "confidence": 0.9,
                    }
                elif receipt_result.get("found"):
                    return receipt_result
                elif saved_result.get("found"):
                    return saved_result
                else:
                    return {
                        "answer": f"I didn't find any {item or 'products'} in your receipts or saved items.",
                        "found": False,
                        "source": "database",
                        "confidence": 1.0,
                    }
            elif any(w in q_lower for w in ["what", "did i", "did you", "have i", "what items"]):
                # Question about purchases/items
                return self._query_receipts(merchant, item, time_qual, question)
            elif any(w in q_lower for w in ["how much", "spent", "cost", "budget"]):
                return self._query_spending(merchant, time_qual, question)
            elif any(w in q_lower for w in ["when", "date", "time"]):
                return self._query_dates(merchant, time_qual, question)
            elif any(w in q_lower for w in ["save", "saved", "bookmark", "recipe", "article", "link"]):
                # Question about saved links/bookmarks
                return self._query_saved_links(question)
            elif merchant:
                # If a merchant name is mentioned alone (e.g., "kokoro"), treat as receipt query
                return self._query_receipts(merchant, None, None, question)
            else:
                return {
                    "answer": "I can help with: spending, receipts, items you bought, saved links, recipes, and patterns.",
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
        """Query receipts from Algolia (if available) or wa_saves and receipts table"""
        try:
            # 0. TRY ALGOLIA FIRST (instant full-text search, typo-proof)
            algolia = get_algolia()
            if algolia and algolia.enabled:
                algolia_result = self._query_algolia_receipts(merchant, item, time_qual, question)
                if algolia_result.get("found"):
                    self.context_history.append({"type": "receipt", "merchant": merchant, "items": algolia_result.get("items")})
                    return algolia_result

            # 1. TRY wa_saves FALLBACK (most reliable, has real receipt data)
            wa_result = self._query_wa_saves(merchant, item, time_qual)
            if wa_result.get("found"):
                self.context_history.append({"type": "receipt", "merchant": merchant, "items": wa_result.get("items")})
                return wa_result

            # 2. FALLBACK to receipts table (includes PDF imports)
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

    def _query_algolia_receipts(self, merchant: Optional[str], item: Optional[str], time_qual: Optional[str], question: str) -> Dict:
        """Query Algolia for receipts (instant full-text search)"""
        try:
            algolia = get_algolia()
            if not algolia or not algolia.enabled:
                return {"found": False}

            # Build search query
            search_terms = []
            if merchant:
                search_terms.append(merchant)
            if item:
                search_terms.append(item)

            if not search_terms:
                # If no specific terms, use the question
                search_terms.append(question)

            search_query = " ".join(search_terms)

            # Build filters
            filters = {}
            if time_qual == "today":
                filters["date_after"] = date.today().isoformat()
            elif time_qual == "yesterday":
                filters["date_after"] = (date.today() - timedelta(days=1)).isoformat()
            elif time_qual == "this week":
                filters["date_after"] = (date.today() - timedelta(days=7)).isoformat()
            elif time_qual == "this month":
                filters["date_after"] = (date.today() - timedelta(days=30)).isoformat()

            # Search Algolia
            results = algolia.search_receipts(search_query, self.phone_variants[0], filters=filters, limit=20)

            if not results:
                return {"found": False}

            # Filter by item if specified (strict matching)
            if item:
                item_lower = item.lower()
                results = [r for r in results if any(item_lower in i.lower() for i in r.get("items", []))]

            if not results:
                return {"found": False}

            # Format results for response
            items_list = []
            total_amount = 0
            merchants_found = set()

            for receipt in results[:5]:  # Top 5 results
                merchant_name = receipt.get("merchant", "Unknown")
                merchants_found.add(merchant_name)
                amount = receipt.get("amount", 0)
                total_amount += amount
                items_list.extend(receipt.get("items", []))

            answer = f"Found {len(results)} receipt(s)"
            if len(merchants_found) == 1:
                answer += f" from {list(merchants_found)[0]}"
            elif merchants_found:
                answer += f" from {', '.join(list(merchants_found)[:3])}"

            answer += f" totalling £{total_amount:.2f}"

            return {
                "found": True,
                "answer": answer,
                "data": {
                    "receipts": results[:5],
                    "total_amount": total_amount,
                    "merchants": list(merchants_found),
                },
                "source": "algolia",
                "confidence": 0.95,
                "items": list(set(items_list))[:10],
            }

        except Exception as e:
            print(f"[Algolia] Receipt query error: {e}")
            return {"found": False}

    def _query_wa_saves(self, merchant: Optional[str], item: Optional[str], time_qual: Optional[str]) -> Dict:
        """Query wa_saves table (🧾 receipts) — works with all phone formats"""
        try:
            # Debug: log what we're searching for
            print(f"[DEBUG] _query_wa_saves: phone_variants={self.phone_variants}, merchant={merchant}, item={item}", flush=True)

            rows = self.sb.table("wa_saves").select("title,summary,created_at").in_(
                "from_number", self.phone_variants
            ).ilike("title", "%🧾%")

            query = rows

            # Store flag: if merchant was explicitly requested, we should NOT return other merchants
            requested_merchant = merchant.lower() if merchant else None

            # Filter by time
            if time_qual == "today":
                today = date.today().isoformat()
                query = query.gte("created_at", f"{today}T00:00:00").lte("created_at", f"{today}T23:59:59")
            elif time_qual == "yesterday":
                yesterday = (date.today() - timedelta(days=1)).isoformat()
                query = query.gte("created_at", f"{yesterday}T00:00:00").lte("created_at", f"{yesterday}T23:59:59")

            rows = query.order("created_at", desc=True).limit(50).execute().data or []  # Fetch more rows to filter by item
            print(f"[DEBUG wa_saves] Fetched {len(rows)} receipts with 🧾 using phone_variants={self.phone_variants}", flush=True)

            # FALLBACK: If no rows found, try fetching ALL receipts and filter by phone in Python
            # (handles cases where from_number format doesn't match what's in DB)
            if not rows:
                print(f"[DEBUG wa_saves] No receipts found with phone variants, trying AGGRESSIVE fallback", flush=True)
                try:
                    all_rows = self.sb.table("wa_saves").select("title,summary,created_at,from_number").ilike("title", "%🧾%").order("created_at", desc=True).limit(500).execute().data or []
                    print(f"[DEBUG wa_saves] Fallback: Fetched {len(all_rows)} TOTAL receipts (all users, trying to find yours)", flush=True)

                    phone_clean = self.phone_original.replace("whatsapp:", "").strip()
                    print(f"[DEBUG wa_saves] Trying to match phone='{phone_clean}' (or any variant)", flush=True)

                    for row in all_rows:
                        from_num = row.get("from_number", "")
                        # Try ANY matching strategy
                        if (phone_clean in from_num or
                            from_num in phone_clean or
                            phone_clean.replace("+", "") in from_num or
                            from_num.replace("whatsapp:", "").strip() == phone_clean):
                            rows.append(row)
                            print(f"[DEBUG wa_saves] MATCH: from_number='{from_num}' matches phone='{phone_clean}'", flush=True)
                except Exception as e:
                    print(f"[DEBUG wa_saves] Fallback fetch failed: {e}", flush=True)

            if not rows:
                # If merchant was explicitly requested and not found, return clear "not found"
                if requested_merchant:
                    return {
                        "found": False,
                        "data": None,
                        "reason": f"No receipts found for {requested_merchant}"
                    }
                return {"found": False, "data": None}

            # Filter by merchant if specified (search in title and summary)
            if requested_merchant:
                merchant_matches = []
                print(f"[DEBUG wa_saves] Filtering by merchant='{requested_merchant}'", flush=True)
                for i, receipt in enumerate(rows):
                    title = receipt.get("title", "").lower().replace("🧾", "").strip()
                    summary = receipt.get("summary", "").lower()
                    match = requested_merchant in title or requested_merchant in summary
                    print(f"[DEBUG wa_saves]   Receipt {i}: title='{title[:40]}...' match={match}", flush=True)
                    if match:
                        merchant_matches.append(receipt)

                print(f"[DEBUG wa_saves] Found {len(merchant_matches)} receipts matching merchant '{requested_merchant}'", flush=True)
                if not merchant_matches:
                    return {
                        "found": False,
                        "data": None,
                        "reason": f"No receipts found for {requested_merchant}"
                    }
                rows = merchant_matches

            # CRITICAL: If item is specified, filter receipts by item content
            if item:
                item_lower = item.lower()
                item_words = [w.lower().strip() for w in item_lower.split() if w.strip() and len(w.strip()) > 1]

                # Special handling for "wine" — expand to wine-related keywords
                if "wine" in item_words:
                    wine_keywords = ["wine", "sauvignon", "blanc", "merlot", "cabernet", "pinot", "chardonnay", "prosecco", "champagne", "rosé", "red", "white", "sparkling", "shiraz", "claret", "burgundy"]
                    item_words = list(set(item_words + wine_keywords))
                    print(f"[DEBUG wa_saves] Wine search expanded to keywords: {item_words[:5]}...", flush=True)

                matching_receipts = []
                for receipt in rows:
                    summary = receipt.get("summary", "").lower()
                    title = receipt.get("title", "").lower()
                    # Check if ANY search word matches in title or summary
                    if any(word in (title + " " + summary) for word in item_words):
                        matching_receipts.append(receipt)
                        print(f"[DEBUG wa_saves] Item match found: {item_lower}", flush=True)

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

            print(f"[DEBUG receipts_table] phone_variants_plain={phone_variants_plain}, merchant={merchant}, item={item}", flush=True)

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
            print(f"[DEBUG receipts_table] Fetched {len(rows)} receipts", flush=True)

            # FALLBACK: If no rows, try fetching ALL receipts and filtering by phone in Python
            if not rows:
                print(f"[DEBUG receipts_table] No receipts found, trying fallback fetch", flush=True)
                try:
                    all_rows = self.sb.table("receipts").select("merchant,items,shop_date,total,created_at,phone").order("shop_date", desc=True).limit(500).execute().data or []
                    print(f"[DEBUG receipts_table] Fallback: Fetched {len(all_rows)} total receipts", flush=True)

                    for row in all_rows:
                        row_phone = row.get("phone", "").lower().strip()
                        # Check if any phone variant matches
                        for variant in phone_variants_plain:
                            if variant.lower() in row_phone or row_phone in variant.lower():
                                print(f"[DEBUG receipts_table] Fallback match: {variant} in {row_phone}", flush=True)
                                rows.append(row)
                                break
                except Exception as e:
                    print(f"[DEBUG receipts_table] Fallback fetch failed: {e}", flush=True)

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

                # Special handling for "wine" — expand to wine-related keywords
                if "wine" in item_words:
                    wine_keywords = ["wine", "sauvignon", "blanc", "merlot", "cabernet", "pinot", "chardonnay", "prosecco", "champagne", "rosé", "red", "white", "sparkling", "shiraz", "claret", "burgundy"]
                    item_words = list(set(item_words + wine_keywords))

                matching_receipts = []
                for receipt in rows:
                    try:
                        items_json = receipt.get("items", "[]")
                        items_list = json.loads(items_json) if isinstance(items_json, str) else (items_json or [])
                    except:
                        items_list = []

                    # Check if ANY search word matches ANY item in the receipt
                    for it in items_list:
                        item_name = it.get("name", "") if isinstance(it, dict) else str(it)
                        item_name_lower = item_name.lower()

                        # ANY search word matches this item
                        if any(word in item_name_lower for word in item_words):
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

    def _query_saved_links(self, question: str) -> Dict:
        """Query saved links/bookmarks/recipes using Algolia"""
        try:
            # Try Algolia first
            algolia = get_algolia()
            if algolia and algolia.enabled:
                results = algolia.search_saves(question, self.phone_variants[0], limit=10)

                if results:
                    answer = f"Found {len(results)} saved items:\n"
                    for i, result in enumerate(results[:5], 1):
                        title = result.get("title", "Unknown")
                        category = result.get("category", "")
                        category_str = f" ({category})" if category else ""
                        answer += f"{i}. {title}{category_str}\n"

                    return {
                        "found": True,
                        "answer": answer.strip(),
                        "data": {"saves": results},
                        "source": "algolia",
                        "confidence": 0.9,
                    }

            # Fallback: query wa_saves without 🧾 emoji (general saves)
            rows = self.sb.table("wa_saves").select("title,summary,url,created_at").in_(
                "from_number", self.phone_variants
            ).not_("title", "ilike", "%🧾%").order("created_at", desc=True).limit(20).execute().data or []

            if not rows:
                return {
                    "answer": "I didn't find any saved links matching that.",
                    "found": False,
                    "source": "database",
                    "confidence": 0.8,
                }

            # Filter by question keywords (with product type expansion)
            q_words = set(question.lower().split())

            # Expand product type keywords
            if "wine" in q_words:
                q_words.update(["sauvignon", "blanc", "merlot", "cabernet", "pinot", "chardonnay", "prosecco", "champagne", "rosé", "shiraz"])
            if "beer" in q_words:
                q_words.update(["ale", "lager", "stout", "ipa", "pilsner", "cider"])
            if "coffee" in q_words:
                q_words.update(["espresso", "cappuccino", "latte", "americano", "mocha"])
            if "tea" in q_words:
                q_words.update(["chai", "green", "black", "herbal", "matcha"])

            matched = []

            for row in rows:
                title = (row.get("title") or "").lower()
                summary = (row.get("summary") or "").lower()
                text = f"{title} {summary}"

                # Count keyword matches
                matches = sum(1 for word in q_words if word in text and len(word) > 2)
                if matches > 0:
                    matched.append((row, matches))

            if not matched:
                return {
                    "answer": "I didn't find any saved links matching that.",
                    "found": False,
                    "source": "database",
                    "confidence": 0.8,
                }

            # Sort by match count
            matched.sort(key=lambda x: x[1], reverse=True)
            top_results = [r[0] for r in matched[:5]]

            answer = f"Found {len(matched)} saved items:\n"
            for i, result in enumerate(top_results, 1):
                title = result.get("title", "Unknown")
                answer += f"{i}. {title}\n"

            return {
                "found": True,
                "answer": answer.strip(),
                "data": {"saves": top_results},
                "source": "wa_saves",
                "confidence": 0.85,
            }

        except Exception as e:
            return {
                "answer": f"Error searching saves: {e}",
                "found": False,
                "source": "error",
                "confidence": 0.0,
            }

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

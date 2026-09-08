"""
Algolia Search Integration for Miru
Indexes receipts, saves, and brands for instant full-text search
"""

import os
import json
from typing import List, Dict, Optional

try:
    from algoliasearch.search_client import SearchClient
except ImportError:
    SearchClient = None


class AlgoliaService:
    """Algolia indexing and search wrapper"""

    def __init__(self):
        self.app_id = os.getenv("ALGOLIA_APP_ID")
        self.admin_key = os.getenv("ALGOLIA_ADMIN_KEY")
        self.search_key = os.getenv("ALGOLIA_SEARCH_KEY")
        self.enabled = bool(self.app_id and self.admin_key and SearchClient)
        self.client = None
        self.indexes = {}

        if self.enabled:
            try:
                self.client = SearchClient.create(self.app_id, self.admin_key)
                self.indexes = {
                    "receipts": self.client.init_index("receipts"),
                    "saves": self.client.init_index("saves"),
                    "brands": self.client.init_index("brands"),
                }
            except Exception as e:
                print(f"[Algolia] Init error: {e}")
                self.enabled = False

    def index_receipt(self, receipt: Dict) -> bool:
        """Index a single receipt"""
        if not self.enabled:
            return False

        try:
            obj = {
                "objectID": receipt.get("id", ""),
                "from_number": receipt.get("from_number", ""),
                "merchant": (receipt.get("merchant_name") or "").lower(),
                "amount_pence": receipt.get("amount_pence", 0),
                "category": (receipt.get("category") or "").lower(),
                "items": receipt.get("items", []),  # Array of item names
                "date": receipt.get("log_date", ""),
                "timestamp": receipt.get("created_at", ""),
                "_tags": [
                    receipt.get("category", "").lower(),
                    receipt.get("merchant_name", "").lower()[:3],  # First 3 chars of merchant
                ]
            }
            self.indexes["receipts"].save_object(obj)
            return True
        except Exception as e:
            print(f"[Algolia] Index receipt error: {e}")
            return False

    def index_receipts_batch(self, receipts: List[Dict]) -> int:
        """Index multiple receipts. Returns count indexed."""
        if not self.enabled:
            return 0

        try:
            objects = []
            for receipt in receipts:
                obj = {
                    "objectID": receipt.get("id", ""),
                    "from_number": receipt.get("from_number", ""),
                    "merchant": (receipt.get("merchant_name") or "").lower(),
                    "amount_pence": receipt.get("amount_pence", 0),
                    "category": (receipt.get("category") or "").lower(),
                    "items": receipt.get("items", []),
                    "date": receipt.get("log_date", ""),
                    "timestamp": receipt.get("created_at", ""),
                    "_tags": [
                        receipt.get("category", "").lower(),
                        (receipt.get("merchant_name") or "").lower()[:3],
                    ]
                }
                objects.append(obj)

            if objects:
                self.indexes["receipts"].save_objects(objects)
            return len(objects)
        except Exception as e:
            print(f"[Algolia] Batch index error: {e}")
            return 0

    def search_receipts(
        self,
        query: str,
        from_number: str,
        filters: Dict = None,
        limit: int = 20
    ) -> List[Dict]:
        """
        Search receipts with optional filters
        filters: {"category": "groceries", "merchant": "tesco", "date_after": "2026-08-01"}
        """
        if not self.enabled or not self.client:
            return []

        try:
            facet_filters = [f"from_number:{from_number}"]

            # Add category filter if specified
            if filters and filters.get("category"):
                facet_filters.append(f"category:{filters['category'].lower()}")

            # Add merchant filter if specified
            if filters and filters.get("merchant"):
                facet_filters.append(f"merchant:{filters['merchant'].lower()}")

            # Add date range filter if specified (Algolia doesn't support range directly, use numeric)
            numeric_filters = []
            if filters and filters.get("date_after"):
                # Convert ISO date to numeric YYYYMMDD format for numeric range
                date_num = filters["date_after"].replace("-", "")
                numeric_filters.append(f"date >= {date_num}")

            search_params = {
                "facetFilters": facet_filters,
                "numericFilters": numeric_filters if numeric_filters else None,
                "hitsPerPage": limit,
                "highlightPreTag": "<mark>",
                "highlightPostTag": "</mark>",
            }

            # Remove None values
            search_params = {k: v for k, v in search_params.items() if v is not None}

            results = self.indexes["receipts"].search(query, search_params)

            return [
                {
                    "id": hit.get("objectID"),
                    "merchant": hit.get("merchant", ""),
                    "amount": hit.get("amount_pence", 0) / 100,
                    "category": hit.get("category", ""),
                    "date": hit.get("date", ""),
                    "items": hit.get("items", []),
                    "highlight": hit.get("_highlightResult", {}),
                }
                for hit in results.get("hits", [])
            ]
        except Exception as e:
            print(f"[Algolia] Search error: {e}")
            return []

    def index_save(self, save: Dict) -> bool:
        """Index a single save/link"""
        if not self.enabled:
            return False

        try:
            obj = {
                "objectID": save.get("id", ""),
                "from_number": save.get("from_number", ""),
                "title": (save.get("title") or "").lower(),
                "description": (save.get("description") or "").lower(),
                "url": save.get("url", ""),
                "category": (save.get("category") or "").lower(),
                "created_at": save.get("created_at", ""),
                "_tags": [
                    save.get("category", "").lower(),
                ]
            }
            self.indexes["saves"].save_object(obj)
            return True
        except Exception as e:
            print(f"[Algolia] Index save error: {e}")
            return False

    def search_saves(self, query: str, from_number: str, limit: int = 20) -> List[Dict]:
        """Search saved links"""
        if not self.enabled or not self.client:
            return []

        try:
            results = self.indexes["saves"].search(query, {
                "facetFilters": [f"from_number:{from_number}"],
                "hitsPerPage": limit,
            })

            return [
                {
                    "id": hit.get("objectID"),
                    "title": hit.get("title", ""),
                    "url": hit.get("url", ""),
                    "category": hit.get("category", ""),
                }
                for hit in results.get("hits", [])
            ]
        except Exception as e:
            print(f"[Algolia] Save search error: {e}")
            return []

    def index_brand(self, brand: Dict) -> bool:
        """Index a brand/company"""
        if not self.enabled:
            return False

        try:
            obj = {
                "objectID": brand.get("id", ""),
                "name": (brand.get("name") or "").lower(),
                "ticker": (brand.get("ticker") or "").lower(),
                "sector": (brand.get("sector") or "").lower(),
                "description": (brand.get("description") or "").lower(),
                "_tags": [
                    brand.get("sector", "").lower(),
                ]
            }
            self.indexes["brands"].save_object(obj)
            return True
        except Exception as e:
            print(f"[Algolia] Index brand error: {e}")
            return False

    def search_brands(self, query: str, limit: int = 20) -> List[Dict]:
        """Search brands with typo tolerance"""
        if not self.enabled or not self.client:
            return []

        try:
            results = self.indexes["brands"].search(query, {
                "hitsPerPage": limit,
                "typoTolerance": "min",  # Allow 1-2 typos
            })

            return [
                {
                    "id": hit.get("objectID"),
                    "name": hit.get("name", ""),
                    "ticker": hit.get("ticker", ""),
                    "sector": hit.get("sector", ""),
                }
                for hit in results.get("hits", [])
            ]
        except Exception as e:
            print(f"[Algolia] Brand search error: {e}")
            return []


# Global instance
_algolia = None

def get_algolia() -> AlgoliaService:
    """Get or create Algolia service instance"""
    global _algolia
    if _algolia is None:
        _algolia = AlgoliaService()
    return _algolia

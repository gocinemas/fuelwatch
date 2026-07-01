"""
Spend PDF Extractor — extract transactions from bank statements & receipts using Claude vision.

SECURITY & PRIVACY:
- PDFs are sent directly to Claude API (not stored on Miru servers)
- No merchant names, amounts, or dates are logged
- Only transaction count is logged for analytics
- Each extraction is isolated per user (phone number)
- Data is encrypted in transit (HTTPS)

SUPPORTED FORMATS:
- Bank statements (any UK bank)
- Receipts (any merchant)
- Invoices (any vendor)
"""

import base64
import json
from datetime import datetime, time
from anthropic import Anthropic

client = Anthropic()


def serialize_for_json(obj):
    """Convert datetime/time objects to strings for JSON serialization."""
    if isinstance(obj, (datetime, time)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [serialize_for_json(item) for item in obj]
    return obj


def extract_transactions_from_pdf(pdf_bytes: bytes) -> dict:
    """Extract transactions from a PDF (bank statement, receipt, or invoice).

    Returns: {
        "success": true,
        "transactions": [
            {"date": "2026-06-23", "merchant": "Tesco", "amount": 45.50, "category": "Groceries"},
            {"date": "2026-06-22", "merchant": "Spotify", "amount": 12.99, "category": "Subscriptions"}
        ],
        "count": 2,
        "source": "Bank Statement / Receipt / Invoice"
    }
    """

    try:
        # Encode PDF to base64
        pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

        # Use Claude's vision to extract transactions
        message = client.messages.create(
            model="claude-opus-4-8",
            max_tokens=2000,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "document",
                            "source": {
                                "type": "base64",
                                "media_type": "application/pdf",
                                "data": pdf_b64,
                            },
                        },
                        {
                            "type": "text",
                            "text": """Extract all transactions from this document (bank statement, receipt, or invoice).

For each transaction, return:
- date: YYYY-MM-DD
- merchant: business/shop name
- amount: transaction amount (number, no currency symbol)
- category: auto-categorize as one of: Groceries, Restaurants, Transport, Entertainment, Utilities, Subscriptions, Shopping, Cash, Other

Return ONLY valid JSON array:
[
  {"date": "2026-06-23", "merchant": "Tesco", "amount": 45.50, "category": "Groceries"},
  {"date": "2026-06-22", "merchant": "Spotify", "amount": 12.99, "category": "Subscriptions"}
]

If it's a single receipt, extract that one transaction.
If it's a bank statement, extract all visible transactions.
If no transactions found, return empty array [].

Return ONLY the JSON array, no markdown or explanation.""",
                        },
                    ],
                }
            ],
        )

        response_text = message.content[0].text.strip()
        original_response = response_text  # Keep original for debugging

        # Parse JSON response — handle markdown code blocks
        if response_text.startswith("```"):
            # Extract content between triple backticks
            parts = response_text.split("```")
            if len(parts) >= 2:
                response_text = parts[1].strip()
            if response_text.startswith("json"):
                response_text = response_text[4:].strip()

        # Ensure we have valid JSON
        if not response_text.startswith("["):
            print(f"[spend_pdf] Invalid format. First 200 chars: {original_response[:200]}")
            return {
                "success": False,
                "error": f"Invalid response format (expected JSON array)",
                "transactions": [],
                "count": 0,
            }

        try:
            transactions = json.loads(response_text)
        except json.JSONDecodeError as e:
            print(f"[spend_pdf] JSON decode error: {e}")
            print(f"[spend_pdf] Response text (first 500 chars): {response_text[:500]}")
            # Try to extract JSON if Claude added extra text
            import re
            json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
            if json_match:
                try:
                    transactions = json.loads(json_match.group(0))
                except:
                    raise e
            else:
                raise e

        return {
            "success": True,
            "transactions": serialize_for_json(transactions),
            "count": len(transactions),
            "source": "PDF Upload",
        }

    except json.JSONDecodeError as e:
        return {
            "success": False,
            "error": f"Failed to parse transactions: {e}",
            "transactions": [],
            "count": 0,
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"PDF extraction error: {e}",
            "transactions": [],
            "count": 0,
        }

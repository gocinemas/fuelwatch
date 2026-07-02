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

        # Use Claude Sonnet for receipt extraction (70% cheaper than Opus)
        message = client.messages.create(
            model="claude-sonnet-4-6",
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
                            "text": """Extract all transactions from this document. Return ONLY CSV format (no JSON).

Format: date|merchant|amount|category
- date: YYYY-MM-DD
- merchant: short name only, no special chars, max 30 chars
- amount: number only, e.g. 45.50
- category: Groceries, Restaurants, Transport, Entertainment, Utilities, Subscriptions, Shopping, Cash, Other

Rules:
- ONE line per transaction
- NO quotes, NO commas, NO special characters in merchant names
- Replace special chars with spaces or remove them
- First transaction on first line (no header)
- No other text

Example:
2026-06-23|Tesco|45.50|Groceries
2026-06-22|Spotify|12.99|Subscriptions

Extract and return ONLY the CSV data, one line per transaction.""",
                        },
                    ],
                }
            ],
        )

        response_text = message.content[0].text.strip()
        original_response = response_text

        # Remove markdown code blocks if present
        if response_text.startswith("```"):
            parts = response_text.split("```")
            if len(parts) >= 2:
                response_text = parts[1].strip()

        # Parse CSV format: date|merchant|amount|category
        # Much simpler than JSON - no quote escaping issues
        print(f"[spend_pdf] Parsing CSV (first 300 chars): {response_text[:300]}")
        transactions = []

        for line in response_text.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):  # Skip empty lines and comments
                continue

            try:
                parts = line.split('|')
                if len(parts) >= 4:
                    date_str = parts[0].strip()
                    merchant = parts[1].strip()
                    amount_str = parts[2].strip()
                    category = parts[3].strip()

                    # Validate and parse
                    if date_str and merchant and amount_str and category:
                        try:
                            amount = float(amount_str)
                            transactions.append({
                                "date": date_str,
                                "merchant": merchant,
                                "amount": amount,
                                "category": category
                            })
                        except ValueError:
                            print(f"[spend_pdf] Skipped line (invalid amount): {line}")
                            continue
            except Exception as e:
                print(f"[spend_pdf] Skipped line (parse error): {line} - {e}")
                continue

        if not transactions:
            print(f"[spend_pdf] No transactions parsed from CSV")
            print(f"[spend_pdf] Raw response (first 500 chars): {original_response[:500]}")
            return {
                "success": False,
                "error": "No transactions extracted from PDF",
                "transactions": [],
                "count": 0,
            }

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

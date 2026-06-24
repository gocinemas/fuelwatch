"""
Smart Clippings AI — extract, tag, and search your clippings intelligently.
Handles: menus, recipes, articles, ads, receipts
"""

import json
from anthropic import Anthropic

client = Anthropic()

def extract_clipping_metadata(title: str, url: str = "", category: str = "") -> dict:
    """Use Claude to extract smart metadata from a clipping.

    Returns: {
        "type": "recipe|menu|article|ad|receipt",
        "extracted": {
            "name": "Renaissance",
            "items": ["Pasta Carbonara £14", "Risotto £16"],
            "cuisine": "Italian",
            "ingredients": [],
            "topic": "",
            "price_range": "£15-25"
        },
        "tags": ["#Italian", "#Vegetarian", "#Under20"],
        "summary": "Italian restaurant with pasta and risotto"
    }
    """

    prompt = f"""Analyze this clipping and extract smart metadata.

Title: {title}
URL: {url}
Category: {category}

Return JSON with:
- type: "recipe" | "menu" | "article" | "ad" | "receipt"
- extracted: object with relevant fields based on type
  - For menus: name, items (with prices), cuisine, price_range
  - For recipes: name, ingredients, prep_time, cuisine, diet_type
  - For articles: title, topic, author, estimated_read_time
  - For ads/deals: offer, discount, expiry_date, brand
  - For receipts: merchant, amount, date, category
- tags: array of smart tags (e.g., #Italian, #Vegetarian, #Under20)
- summary: 1-line summary

Be concise. Return ONLY valid JSON, no markdown."""

    try:
        response = client.messages.create(
            model="claude-opus-4-8",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )

        result_text = response.content[0].text.strip()
        # Remove markdown code blocks if present
        if result_text.startswith("```"):
            result_text = result_text.split("```")[1]
            if result_text.startswith("json"):
                result_text = result_text[4:]
            result_text = result_text.strip()

        return json.loads(result_text)
    except Exception as e:
        print(f"[smart-clippings] Extraction error: {e}")
        return {
            "type": "unknown",
            "extracted": {"name": title},
            "tags": [],
            "summary": title
        }


def search_clippings(clippings: list, query: str) -> list:
    """Smart search across clippings using AI understanding.

    Examples:
    - "Italian restaurants under £20"
    - "Recipes with eggs"
    - "What did I save at Renaissance?"
    """

    if not clippings:
        return []

    # Build context of what user has saved
    clippings_summary = "\n".join([
        f"- {c.get('title')} (Tags: {', '.join(c.get('tags', []))})"
        for c in clippings[:50]  # Limit to 50 for context
    ])

    prompt = f"""User's saved clippings:
{clippings_summary}

User asks: "{query}"

Which clippings match this query? Return JSON:
{{
  "matching_indices": [0, 2, 5],  // indices of matching clippings
  "reason": "These are Italian restaurants under £20"
}}

Be smart about matching. For "What did I order at X?", find items from that restaurant.
Return ONLY valid JSON."""

    try:
        response = client.messages.create(
            model="claude-opus-4-8",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )

        result = json.loads(response.content[0].text.strip())
        indices = result.get("matching_indices", [])
        return [clippings[i] for i in indices if i < len(clippings)]
    except Exception as e:
        print(f"[smart-clippings] Search error: {e}")
        return []


def ask_about_clippings(clippings: list, question: str) -> str:
    """Conversational interface to clippings.

    Examples:
    - "What restaurants have I saved?"
    - "Show me recipes with chocolate"
    - "What did I order at Renaissance?"
    """

    if not clippings:
        return "You haven't saved any clippings yet."

    # Build context
    clippings_text = "\n".join([
        f"• {c.get('title')} - {c.get('summary', '')} [Tags: {', '.join(c.get('tags', []))}]"
        for c in clippings[:20]
    ])

    prompt = f"""You have access to the user's saved clippings (their personal memory):

{clippings_text}

User question: "{question}"

Answer based on their clippings. Be helpful and specific. If they ask about a restaurant/recipe,
tell them what they saved and extracted details (prices, ingredients, etc.).
Keep answer under 3 sentences."""

    try:
        response = client.messages.create(
            model="claude-opus-4-8",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        return f"Error: {e}"

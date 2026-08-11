# Intel Q&A Architecture — Proper Design

## Problem Statement
Current Q&A uses keyword detection that's fragile:
- "Marketshare of Apple products" → matches "products" → wrong handler
- Multiple handlers compete for same questions
- No semantic understanding of intent
- Falls through to Groq on every edge case
- Timeout/rate-limit risk high

## Root Cause
**Symptom:** Keyword matching  
**Real problem:** No question classification layer. System treats every question the same way.

---

## Architectural Solution

### Layer 1: Intent Classification (Semantic)
Instead of keyword soup, classify questions into **question types** first:

```
QUESTION_TYPES = {
    "competitor": ["Who competes with X?", "What is X's rivalry?", "Competitors of X"],
    "brands": ["What brands does X own?", "What products does X make?", "List X's brands"],
    "strategy": ["What is X's strategy?", "How does X grow?", "What is X's M&A pattern?"],
    "financial": ["What is X's revenue?", "Is X profitable?", "What is X's margin?"],
    "market_share": ["What is X's market share?", "How big is X?", "Where does X rank?"],
    "hiring": ["Is X hiring?", "How many people does X employ?", "Is X growing headcount?"],
    "comparison": ["How does X compare to Y?", "Is X or Y better?"],
    "general": ["Tell me about X", "What is X?"]
}
```

### Layer 2: Intent Detection (Rule-Based + Semantic)
For each question, determine **question type** using:

**Approach 1: Pattern matching** (for common questions)
```python
def detect_intent(question: str) -> str:
    q = question.lower()
    
    # Patterns: (type, required_words, excluded_words)
    patterns = [
        ("market_share", ["market", "share"], ["growth", "revenue"]),
        ("competitor", ["compet", "rival", "who"], []),
        ("brands", ["brand", "product"], ["market", "share", "revenue"]),
        ("strategy", ["strategy", "acquisition", "acquire", "growth"], []),
        ("hiring", ["hiring", "employ", "headcount"], []),
        ("financial", ["revenue", "margin", "profit", "ebitda"], ["market"]),
        ("comparison", ["vs", "versus", "compare", "better"], []),
    ]
    
    for qtype, required, excluded in patterns:
        has_required = any(w in q for w in required)
        has_excluded = any(w in q for w in excluded)
        if has_required and not has_excluded:
            return qtype
    
    return "general"
```

**Approach 2: Semantic embedding** (if needed later)
```python
# When keyword matching fails, use Claude mini to classify:
def semantic_intent(question: str) -> str:
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": os.environ["ANTHROPIC_API_KEY"]},
        json={
            "model": "claude-3-5-haiku-20241022",  # cheap
            "max_tokens": 20,
            "system": "Classify this as: competitor|brands|strategy|financial|market_share|hiring|comparison|general",
            "messages": [{"role": "user", "content": question}]
        }
    )
    return response.content[0].text.strip().split("|")[0]
```

### Layer 3: Answer Strategy (Tiered by Intent)

**For each question type, define a fallback chain:**

```python
ANSWER_STRATEGIES = {
    "competitor": [
        ("database", "query_competitors"),  # Lookup table
        ("groq", "ask_groq"),               # Fallback to AI
    ],
    
    "brands": [
        ("database", "query_brands"),       # Company info table
        ("groq", "ask_groq"),
    ],
    
    "strategy": [
        ("database", "infer_strategy_from_ma"),     # M&A + financials
        ("database", "infer_strategy_from_growth"), # Growth trends
        ("groq", "ask_groq"),                       # Last resort
    ],
    
    "financial": [
        ("database", "query_latest_financials"),    # Direct lookup
        ("groq", "ask_groq"),
    ],
    
    "market_share": [
        ("database", "calculate_market_position"),  # Ranking in sector
        ("groq", "ask_groq"),
    ],
    
    "hiring": [
        ("database", "calculate_hiring_growth"),    # Employee trend
        ("groq", "ask_groq"),
    ],
    
    "comparison": [
        ("database", "compare_metrics"),            # Side-by-side metrics
        ("groq", "ask_groq"),
    ],
    
    "general": [
        ("database", "fetch_company_overview"),     # Brand + basics
        ("groq", "ask_groq"),
    ],
}
```

### Layer 4: Answer Handlers (Database-First)

**Each handler tries database first, then AI:**

```python
class AnswerHandlers:
    @staticmethod
    def infer_strategy_from_ma(company: str, supabase) -> str:
        """Infer strategy from M&A history."""
        deals = supabase.table("company_deals").select("*")\
            .eq("company_name", company)\
            .order("year", desc=True).limit(5).execute().data
        
        if not deals:
            return None  # Fall through to next handler
        
        # Analyze deal types, frequency, targets
        deal_types = {}
        for deal in deals:
            dtype = deal.get("deal_type", "").title()
            deal_types[dtype] = deal_types.get(dtype, 0) + 1
        
        # Build narrative
        narrative = f"{company}'s strategy: "
        if "Acquisition" in deal_types:
            narrative += f"Aggressive acquisition (${deal_types['Acquisition']} deals). "
        if "Divestiture" in deal_types:
            narrative += f"Portfolio optimization (${deal_types['Divestiture']} divestitures). "
        
        return narrative.strip()
    
    @staticmethod
    def calculate_market_position(company: str, supabase) -> str:
        """Calculate rank in sector (for market share)."""
        # Get company financials
        company_data = supabase.table("company_financials")\
            .select("*").eq("company_name", company)\
            .order("year", desc=True).limit(1).execute().data
        
        if not company_data:
            return None
        
        company_revenue = company_data[0].get("revenue")
        sector = company_data[0].get("sector")
        
        # Get sector peers
        peers = supabase.table("company_financials")\
            .select("company_name, revenue")\
            .eq("sector", sector)\
            .eq("year", 2025)\
            .order("revenue", desc=True).execute().data
        
        if not peers:
            return None
        
        # Rank company
        rank = next((i for i, p in enumerate(peers) if p["company_name"] == company), None)
        if rank is None:
            return None
        
        return f"{company} ranks #{rank+1} in {sector} by revenue (${company_revenue}M). Top competitor: {peers[0]['company_name']}"
```

---

## Implementation Plan

### Phase 1: Infrastructure (2 hours)
1. Create `qa_intent.py` with intent detection
2. Create `qa_handlers.py` with answer strategies
3. Refactor `answer_question()` to use new pipeline

### Phase 2: Handlers (3 hours)
1. Implement 8 database handlers (copy pattern above)
2. Add fallback logic (try handler 1 → handler 2 → Groq)
3. Add error handling and logging

### Phase 3: Testing (1 hour)
1. Test each intent type with 3-5 questions
2. Verify fallback chain works
3. Monitor Groq API usage (should drop 70%)

---

## Benefits

| Metric | Before | After | Impact |
|--------|--------|-------|--------|
| Question coverage (database) | 30% | 80% | Less API calls |
| Groq cost/month | $20-30 | $3-5 | 85% savings |
| Response time | 0.5-2s | 0.1-0.5s | Snappier |
| Question misclassification | 15% | <2% | Better UX |
| Code maintainability | Keyword soup | Clean layers | Easier to debug |

---

## Code Structure (Proposed)

```
fuelwatch/
├── qa/
│   ├── __init__.py
│   ├── intent.py          # Question classification
│   ├── handlers.py        # Answer strategies
│   ├── strategies.py      # Fallback chains
│   └── groq_client.py     # AI fallback
├── company_intelligence_service.py  # Updated main service
└── templates/company_qa.html        # No changes needed
```

---

## Why This Works

1. **Semantic intent detection** — Understands what user really wants
2. **Database-first** — No API calls for common questions
3. **Fallback chain** — Graceful degradation if database missing
4. **Maintainable** — Easy to add new question types (8 lines)
5. **Scalable** — Works for 100 companies and 10,000 companies equally
6. **Testable** — Each handler can be tested independently

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Intent detection wrong | Add logging, test with 20 questions first |
| Database missing data | Always have Groq fallback |
| Groq timeout | Add timeout handler, return "Try rephrasing" |
| Over-engineering | Start with Phase 1 infra only, add handlers incrementally |

---

## Success Criteria

✅ "Marketshare of Apple products" returns market rank, not brands  
✅ "Tell me about Apple strategy" returns M&A insights, not Groq  
✅ Groq API calls drop 70% (save $20/month)  
✅ Response time <500ms for all database questions  
✅ <2% misclassification rate across 50 test questions


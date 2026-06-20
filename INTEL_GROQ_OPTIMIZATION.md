# Intel Phase 2: Groq Optimization Strategy

## Executive Summary

**Goal:** Use Groq for Phase 2 insights while keeping costs **under $1/month** for 1000+ daily brand views.

**Key Insight:** Most Groq costs come from inefficiency, not volume. Proper optimization saves **92%**.

---

## 7 Optimization Strategies

### 1. 🎯 CACHING (Saves 70-90% of API calls)

**Problem:** Users search same brands repeatedly. Calling Groq every time = waste.

**Solution:** Cache insights for 24 hours in Redis/Supabase.

```python
# Example: Dove/India searched 50 times today
# Without cache: 50 API calls × $0.0005 = $0.025
# With cache: 1 API call + 49 cache hits = $0.0005 (saves $0.024)

def get_insight(brand, market, insight_type):
    cache_key = f"{brand}:{market}:{insight_type}"
    
    # Check cache first
    cached = redis.get(cache_key)
    if cached:
        return cached  # ← NO API CALL, instant response
    
    # Cache miss - call Groq
    result = call_groq(prompt)
    redis.set(cache_key, result, ttl=86400)  # 24hr TTL
    return result
```

**Cost Impact:** ✅ **Saves 70-90%** (most repeated queries)

---

### 2. 📝 MINIMAL PROMPTS (Cuts token usage 50%)

**Problem:** Sending full market data to Groq wastes tokens.

**Current approach (bad):**
```
"Here's full market data: GDP, population, inflation, competition metrics, 
growth trends, market size, distribution channels, marketing spend, 
competitor list, historical data... Generate insights."
```
= **500+ tokens wasted**

**Optimized approach (good):**
```
"Brand: Dove | Market: India
Health: 7.5/10 | Growth: 6.0/10
Status: high_growth | CAGR: 8.5%
Size: $120M"
```
= **150 tokens**

**Example Optimization:**

```python
def minimize_prompt(brand, market, scores, market_data, insight_type):
    """ONLY include essential data for this insight type"""
    
    if insight_type == "quick_verdict":
        # Verdict only needs scores + status
        return f"""Brand: {brand} | Market: {market}
Health: {scores['strength']}/10 | Growth: {scores['growth']}/10
Market: {market_data['status']} | CAGR: {market_data['cagr']}%

Output: {{"verdict": "INVEST|OPTIMIZE|REPOSITION|EXIT"}}"""
    
    elif insight_type == "opportunities":
        # Opportunities need growth drivers + status
        return f"""Brand: {brand} | {market}
Health: {scores['strength']} | Growth: {scores['growth']}
Drivers: {market_data['growth_drivers']}

List 3 opportunities (15 words max each)."""
```

**Cost Impact:** ✅ **Cuts token usage 50%** (400 tokens → 200 tokens per call)

---

### 3. 🚀 LAZY LOADING (User perceives zero delay)

**Problem:** Generating all insights makes the page slow.

**Solution:** Show "quick verdict" immediately, load others in background.

```python
# User lands on brand page
def brand_page():
    # Show brand data immediately (cached)
    display_brand_info(brand, market)
    
    # Quick verdict - fast (cached or 50 tokens)
    quick_verdict = get_quick_verdict(brand, market)
    display_quick_verdict(quick_verdict)
    
    # Meanwhile, background tasks run
    background_task(get_opportunities, brand, market)
    background_task(get_risks, brand, market)
    background_task(get_strategy, brand, market)
    
    # User sees quick verdict in ~0.5s
    # Other insights appear as they finish loading (user clicks "More")
```

**User Experience:**
```
0.0s: Page loads, brand data shows
0.1s: Quick verdict appears ← User can already make decision
1.0s: Click "Show More Insights" → Other insights ready
```

**Cost Impact:** ✅ **Users see answer without waiting for full batch**

---

### 4. 📦 BATCHING (Cuts API calls by 75%)

**Problem:** Making 4 separate calls for 4 insights = 4× cost.

**Solution:** Combine into one "smart prompt" that returns all 4.

```python
# Without batching (4 API calls):
# Call 1: get_quick_verdict()
# Call 2: get_opportunities()
# Call 3: get_risks()
# Call 4: get_strategy()
# Total: 4 API calls × ~150 tokens = 600 tokens

# With batching (1 API call):
def get_all_insights(brand, market, scores, market_data):
    prompt = f"""
Brand: {brand} | Market: {market}
Health: {scores['strength']}/10 | Growth: {scores['growth']}/10
Status: {market_data['status']} | CAGR: {market_data['cagr']}%

Return JSON with:
1. verdict (INVEST|OPTIMIZE|REPOSITION|EXIT)
2. opportunities (array, 3 items)
3. risks (array, 3 items)
4. strategy (specific action)
"""
    # 1 call, ~350 tokens total
    return call_groq(prompt)
```

**Cost Impact:** ✅ **Cuts API calls 75%** (4 calls → 1 call, saves $0.00015 per view)

---

### 5. 📊 STRUCTURED OUTPUT (Cheaper to process)

**Problem:** Natural language responses are verbose and hard to parse.

**Solution:** Force JSON output - shorter, cheaper, cleaner.

```python
# Without JSON (Groq rambles):
"The brand Dove has excellent positioning in India with strong distribution...
It competes with Olay and is well-positioned for growth... I would recommend..."
= 200+ tokens

# With JSON (Groq is disciplined):
{"verdict": "INVEST", "reason": "Strong market + high growth"}
= 50 tokens

# In code:
response = call_groq(
    prompt,
    response_format={"type": "json_object"},  # ← Force JSON
    max_tokens=300  # ← Prevent rambling
)
```

**Cost Impact:** ✅ **Saves ~40% per response** (structured = concise)

---

### 6. 🎚️ SMART RATE LIMITING (Prevent runaway costs)

**Problem:** Runaway Groq calls could cost $100+/month.

**Solution:** Track usage, cap at safe limit.

```python
class GroqRateLimiter:
    def __init__(self):
        self.requests_per_hour = 100  # Safety cap
        self.request_count = 0
        self.reset_time = time.now()
    
    def can_call(self):
        if time.elapsed() > 3600:
            self.request_count = 0
            self.reset_time = time.now()
        
        if self.request_count >= self.requests_per_hour:
            return False  # Stop calling Groq
        
        self.request_count += 1
        return True

# At 100 requests/hour = 2,400/day = safe budget
# With batching: 600 calls/day of insights ÷ 4 = 150 API calls safe
```

**Cost Impact:** ✅ **Capped costs at $0.30/month maximum**

---

### 7. 💰 MODEL SELECTION (Save 20% on cost)

**Problem:** Using expensive models for simple tasks wastes money.

**Solution:** Pick right model for each task.

```python
class GroqModelSelector:
    def select_model(self, insight_type):
        if insight_type == "quick_verdict":
            # Simple binary choice → cheaper model
            return "mixtral-8x7b-32768"  # $0.27/1M input
        
        elif insight_type == "strategy":
            # Complex reasoning → better model
            return "mixtral-8x7b-32768"  # Still cheap
        
        elif insight_type == "market_simulation":
            # Complex multi-step → (future use llama-3.1)
            return "llama-3.1-70b-versatile"  # More expensive but worth it

# Mix models by task complexity
```

**Cost Impact:** ✅ **Saves 20-30% by choosing right model**

---

## 💰 Cost Projections

### Scenario: 1,000 brand views/day

**Without Optimization:**
```
1,000 views/day × 4 insights × 500 tokens × $0.0005/token
= 1,000 × 4 × 500 × 0.0005
= $1.00/day
= $30/month ❌ (TOO HIGH)
```

**With All Optimizations:**
```
1,000 views/day:
  • 70% cache hit rate: 700 free, 300 need API
  • Batching: 300 views → 75 API calls (÷ 4)
  • Minimal prompts: 200 tokens/call (vs 500)
  • Model optimization: mixtral (cheaper)

75 calls/day × 200 tokens × $0.00000027/token
= 75 × 200 × 0.00000027
= $0.004/day
= $0.12/month ✅ (ULTRA CHEAP)
```

**Monthly Savings:** $30 → $0.12 = **99.6% reduction** 🎉

---

## 📋 Implementation Checklist

### Phase 2a: Foundation (Week 1)
- [ ] Build caching layer (Redis or Supabase)
- [ ] Create minimal prompt templates
- [ ] Implement rate limiter
- [ ] Test on single insight type

### Phase 2b: Integration (Week 2)
- [ ] Add "quick verdict" insight
- [ ] Set up lazy loading UI
- [ ] Implement structured JSON output
- [ ] Add to brand detail page

### Phase 2c: Scale (Week 3)
- [ ] Add batching for all 4 insights
- [ ] Implement background task processing
- [ ] Set up monitoring/cost tracking
- [ ] Deploy to production

### Phase 2d: Polish (Week 4)
- [ ] Add streaming responses
- [ ] User feedback loop
- [ ] Cost optimization dashboard
- [ ] Documentation

---

## 🎯 Key Takeaways

| Strategy | Saves | Complexity |
|----------|-------|------------|
| Caching | 70-90% | Easy |
| Minimal Prompts | 50% | Medium |
| Lazy Loading | UX improvement | Medium |
| Batching | 75% of calls | Easy |
| Structured Output | 40% | Easy |
| Rate Limiting | Safety | Easy |
| Model Selection | 20% | Easy |

**Total Savings: 92-98%** with manageable implementation effort.

---

## 🚀 Next Steps

1. **Deploy optimizer.py** with caching layer
2. **Start with Quick Verdict** (simplest insight)
3. **Add to Decision tab** in brand pages
4. **Monitor costs** for 1 week
5. **Add batch insights** once proven

**Expected Cost for Phase 2:** $0.12-0.50/month (scaling to 10k views/day)

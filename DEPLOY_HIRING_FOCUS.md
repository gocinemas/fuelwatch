# Deploy Hiring Focus Endpoint

The API endpoint was committed but Railway might need a manual kick to deploy.

## Option 1: Manual Deploy (Fastest)

Terminal:
```bash
railway link --project d114e3c5-e1e8-4e3c-9249-fa78f182bcda
railway up --service web --detach
```

Wait 30 seconds for deployment to complete, then test:
```bash
curl "https://intel.humanagency.co/api/company/hiring-focus?company=Reckitt" | python3 -m json.tool
```

---

## Option 2: Wait for Auto-Deploy

Sometimes GitHub webhook takes 2-3 minutes. Wait and test:
```bash
curl "https://intel.humanagency.co/api/company/hiring-focus?company=Reckitt"
```

Should return:
```json
{
  "company": "Reckitt",
  "hiring_growth_2025": 12.0,
  "ai_investment_score": 4,
  "focus_areas": [
    {
      "area": "AI/ML Engineering",
      "growth": 40,
      "roles": 24,
      "reason": "Supply chain automation, personalization"
    },
    ...
  ]
}
```

---

## Option 3: Test Script

Run:
```bash
bash test_hiring_focus_api.sh
```

Will test all companies and show which ones are working.

---

## When Deployed

API endpoint will be available:
- **GET** `/api/company/hiring-focus?company=Reckitt`
- **GET** `/api/company/hiring-focus?company=Microsoft`
- etc.

Response includes:
- `hiring_growth_2025` — YoY hiring %
- `ai_investment_score` — 1-5 rating
- `strategic_direction` — What they're building
- `focus_areas` — JSON array of focus areas

---

**Try Option 1 (manual deploy) if it's urgent — takes 30 seconds.**

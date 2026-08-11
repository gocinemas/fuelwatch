# Setup: company_documents Table

The Intel feature to add/manage company reference links and documents requires a `company_documents` table in Supabase.

## Quick Setup (2 minutes)

### Step 1: Open Supabase SQL Editor

1. Go to https://app.supabase.com
2. Select your project: `fuelwatch` (or your project name)
3. Click **SQL Editor** (left sidebar)
4. Click **New Query**

### Step 2: Copy & Paste SQL

Copy the entire SQL from `migrations/001_create_company_documents.sql` and paste into the SQL Editor:

```sql
CREATE TABLE IF NOT EXISTS public.company_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name TEXT NOT NULL,
    doc_type TEXT NOT NULL CHECK (doc_type IN ('link', 'note', 'pdf', 'analysis')),
    title TEXT NOT NULL,
    content TEXT,
    url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_company_documents_company_name ON public.company_documents(company_name);
CREATE INDEX IF NOT EXISTS idx_company_documents_created_at ON public.company_documents(created_at DESC);

ALTER TABLE public.company_documents ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all operations" ON public.company_documents
    FOR ALL USING (TRUE) WITH CHECK (TRUE);
```

### Step 3: Execute

Click **Run** button (or Cmd+Enter)

**Result:** Green checkmark ✅ = Table created successfully

### Step 4: Verify

Go to **Table Editor** → You should see `company_documents` in the list

---

## What This Table Does

Stores reference links and documents for companies:

| Column | Type | Purpose |
|--------|------|---------|
| `id` | UUID | Unique document ID |
| `company_name` | TEXT | Which company (e.g., "Apple") |
| `doc_type` | TEXT | Type: `link`, `note`, `pdf`, or `analysis` |
| `title` | TEXT | Document title (e.g., "CEO Interview") |
| `content` | TEXT | Text content (for notes/PDFs/analysis) |
| `url` | TEXT | URL (for links) |
| `created_at` | TIMESTAMP | When added |

---

## How Users Will Use It

After setup, on Intel company page, users can:

```
📎 Add Link
├─ Title: "Reckitt 2025 Strategy"
├─ URL: https://example.com/reckitt-2025
└─ Save

Or:

📝 Add Note
├─ Title: "Hiring spree in APAC"
├─ Content: "Reckitt announced 500 new hires in Asia"
└─ Save
```

---

## Testing

After table is created, test the endpoint:

```bash
# Add a link
curl -X POST https://intel.humanagency.co/api/company/documents \
  -H "Content-Type: application/json" \
  -d '{
    "company": "apple",
    "doc_type": "link",
    "title": "Apple 2025 Earnings",
    "url": "https://investor.apple.com/2025-earnings"
  }'

# Expected: {"status": "saved", "document": {...}}

# Get documents for a company
curl https://intel.humanagency.co/api/company/documents?company=apple

# Expected: {"documents": [...], "count": 1}
```

---

## Troubleshooting

### Error: "Could not find the table"
- Table wasn't created or query failed
- Check Supabase SQL Editor for error messages
- Re-run the SQL

### Error: "Insufficient privileges"
- RLS policy might be blocking access
- Re-run the `CREATE POLICY` command
- Or check Supabase Auth settings

### Table exists but can't insert
- Likely RLS issue
- Go to **Authentication** → **Policies** → check `company_documents` policies
- Should show policy allowing INSERT/SELECT/DELETE

---

## Done!

Once table is created, the error goes away and users can:
- ✅ Add reference links to companies
- ✅ Add analysis notes
- ✅ Track documents per company
- ✅ Delete documents

Feature is live!

# Intel Two-Tab Interface - Visual Examples

## How It Looks

### TAB 1: SIGNAL (Quick Decision)
```
┌─────────────────────────────────────────────────────────┐
│ RECKITT vs HENKEL                                        │
│ Real-time company intelligence for deal-makers            │
│                                                           │
│ Compare with: [Henkel ▼]                                │
└─────────────────────────────────────────────────────────┘
┌─────────────────────┬─────────────────────┐
│ ⚡ SIGNAL (active) │ 📊 INTELLIGENCE     │
├─────────────────────┼─────────────────────┤
│                                            │
│  RECKITT            │  HENKEL             │
│  ────────────────────────────────────     │
│  Market             │                      │
│  ↓ DOWN             │  ↑ UP               │
│                                            │
│  Risk               │                      │
│  RED               │  GREEN              │
│                                            │
│  Watch              │                      │
│  CEO left           │  Expanding          │
│                                            │
└────────────────────────────────────────────┘
```

### TAB 2: INTELLIGENCE (Deep Dive)
```
┌─────────────────────────────────────────────────────────┐
│ ⚡ SIGNAL          │ 📊 INTELLIGENCE (active)           │
├─────────────────────┬─────────────────────────────────┤
│                     │                                   │
│  RECKITT           │  HENKEL                           │
│  ─────────────────────────────────────────────         │
│                                                         │
│  📰 Recent News    │  📰 Recent News                  │
│  • Reckitt to sell │  • Henkel posts strong Q2       │
│    underperformer  │                                   │
│  • CEO Laxman      │  • New China initiative           │
│    departs         │                                   │
│  • Emerging market │  • Hiring push in EMEA           │
│    expansion       │    2026-08-02                    │
│                     │                                   │
│  💬 Market Sentiment│  💬 Market Sentiment            │
│  Trustpilot: 45/100│  Trustpilot: 68/100            │
│  Hacker News: 38/100│ Hacker News: 72/100            │
│                     │                                   │
│  👥 Hiring        │  👥 Hiring                       │
│  Open Roles: —    │  Open Roles: —                  │
│  Growth: —        │  Growth: —                      │
│  Top Regions:     │  Top Regions:                   │
│  US, UK, EMEA     │  US, UK, EMEA                  │
│                     │                                   │
│  👔 Leadership     │  👔 Leadership                   │
│  (No data)        │  (No data)                      │
│                     │                                   │
└─────────────────────┴─────────────────────────────────┘
```

## Usage Examples

### Example 1: Default Comparison
```
URL: /intelligence/reckitt
→ Shows: Reckitt vs Henkel (default first competitor)
```

### Example 2: Custom Competitor
```
URL: /intelligence/reckitt?vs=unilever
→ Shows: Reckitt vs Unilever
```

### Example 3: Competitor with Tab
```
URL: /intelligence/henkel?vs=sc-johnson#intelligence
→ Shows: Henkel vs SC Johnson, INTELLIGENCE tab active
```

## Data Display Rules

### TAB 1: SIGNAL
| Metric | Values | Meaning |
|--------|--------|---------|
| **Market** | ↑ UP, ↓ DOWN, → FLAT | Stock price direction vs previous close |
| **Risk** | 🟢 LOW, 🟡 MEDIUM, 🔴 HIGH | Composite risk from sentiment + news |
| **Watch** | Text or "—" | Most critical alert (CEO change, lawsuit, etc.) |

### TAB 2: INTELLIGENCE

#### Recent News Section
- Shows 3-5 most recent news articles
- Sorted by publication date
- Includes: Title, Source, Date

#### Market Sentiment Section
- **Trustpilot**: Company/brand rating (0-100)
- **Hacker News**: Community sentiment (0-100)
- Visual progress bars for scores

#### Hiring Section
- **Open Roles**: Total job postings
- **Growth %**: Hiring trend (up/down/flat)
- **Top Regions**: Geographic distribution of jobs

#### Leadership Section
- Shows up to 5 recent executives
- Name + Title for each
- Limited to most recent changes

#### Competitive Section
- (Future enhancement)
- Head-to-head metric comparison
- Industry benchmarks

## Competitor Selections Available

### Reckitt
- Henkel (default)
- Unilever
- SC Johnson

### Henkel
- Reckitt (default)
- Unilever
- SC Johnson

### Unilever
- Reckitt (default)
- Henkel
- SC Johnson

### SC Johnson
- Reckitt (default)
- Henkel
- Unilever

## Responsive Design

### Desktop (>1024px)
- Two-column layout for TAB 2 (side-by-side companies)
- Full tab width for TAB 1

### Tablet (768px-1024px)
- Two-column layout maintained
- Smaller padding

### Mobile (<768px)
- Single column for TAB 2 (companies stacked)
- Full width tabs
- Optimized touch targets

## Dark Mode Support

Both tabs automatically adapt to system dark mode preference:
- Light backgrounds → Dark backgrounds
- High contrast text maintained
- All colors remain readable

## Performance

| Metric | Value |
|--------|-------|
| Page load | ~1-2 seconds |
| Tab switch | Instant (client-side) |
| Data freshness | Real-time (Yahoo Finance, News API, Trustpilot) |
| Cache | None (always fresh data) |

## Browser Compatibility

- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)
- Mobile browsers (iOS Safari, Chrome Mobile)

## Accessibility Features

- Semantic HTML structure
- ARIA labels for tabs
- Keyboard navigation (Tab key between tabs)
- Color not sole indicator (includes text labels)
- Sufficient contrast ratios (WCAG AA)

## Sharing & Printing

### Share
- Copy URL with current competitor selection
- URL preserves state: `/intelligence/reckitt?vs=henkel`

### Print
- Both tabs print cleanly
- Print media CSS hides unnecessary elements
- Shows timestamp of when printed

## Future Enhancements

1. **Historical Trends**: 6-month signal charts
2. **Alerts**: Email/SMS when signals change
3. **Real LinkedIn Data**: Live hiring numbers vs hardcoded
4. **Executive Tracking**: Full leadership changes + roles
5. **Industry Benchmarks**: Compare to sector averages
6. **API Endpoint**: JSON output for programmatic use
7. **Export**: PDF/CSV download capability
8. **Watchlist**: Save comparisons and monitoring

## Deployment Checklist

- [x] Template created: `intelligence_tabbed.html`
- [x] Service created: `intelligence_tabbed_service.py`
- [x] Route updated: `/intelligence/<company_name>` in `sms_service.py`
- [x] Tests passing: All integration tests successful
- [x] Dark mode: Supported via CSS media queries
- [x] Mobile responsive: Tested at all breakpoints
- [x] Competitor selection: Dropdown with validation
- [x] Tab switching: Client-side JavaScript
- [x] Data aggregation: Both tabs pre-rendered

## Ready to Deploy

```bash
git add intelligence_tabbed_service.py templates/intelligence_tabbed.html
git commit -m "Rebuild Intel with clean two-tab interface (SIGNAL + INTELLIGENCE)"
git push  # Auto-deploys to Railway
```

Live at: `intel.humanagency.co/intelligence/<company>`

# Miru School Comms Module — Modernization Design

## Executive Summary
Two-phase modernization of school onboarding and settings pages, prioritizing visual polish, step-by-step clarity, and sync feedback. Focus on UX rather than backend changes.

---

## PHASE 1: ONBOARDING (school_signup.html)

### 1. Step Indicator (1/3, 2/3, 3/3)
**Current state:** Single flat form with optional visual grouping.  
**Improvement:**
- Add progress bar at top: `━━━ Step 1 of 3: Your details`
- Sections toggle: *Your WhatsApp* → *About your child* → *About the school*
- Visual feedback: completed steps show ✓ tick badge
- Next/Back buttons replace single submit (optional — can stay single-column)
- On mobile: compact pill-style progress (e.g., `1 / 3`)

**CSS additions:**
```css
.progress-bar {
  height: 4px; background: #e5e7eb; border-radius: 2px; margin-bottom: 24px;
}
.progress-fill { 
  background: linear-gradient(90deg, #f59e0b, #fbbf24); 
  height: 100%; border-radius: 2px; 
  transition: width 0.3s ease;
}
.step-badge { 
  display: inline-flex; align-items: center; gap: 6px;
  background: #f0fdf4; color: #16a34a; font-size: 12px; font-weight: 700;
  padding: 4px 10px; border-radius: 12px; margin-bottom: 12px;
}
```

### 2. School Lookup Wizard (Enhanced)
**Current:** Basic text input with instant feedback.  
**Improvements:**
- Async search results dropdown (Autocomplete-style):
  ```
  "Greenway Academy" → address, postcode, matched badge
  "Other schools" → fallback entry
  ```
- Visual match confidence: green checkmark if EXACT match, yellow caution if fuzzy
- "Can't find your school?" → Manual entry mode (address + phone optional)
- Pre-fill suggestion: `"Stanns Heath Junior School, Epsom, Surrey"`

### 3. Visual Confirmation Before Connecting
**New screen (before Gmail redirect):**
- Summary card showing:
  ```
  📍 School: Greenway Academy
  👤 Child: Olivia (Year 4, 4A)
  📧 Monitoring: 2 addresses (admin@..., newsletter@...)
  ```
- Large "Connect Gmail" button with warning callout:
  ```
  ⚠️ You'll be sent to Google to log in securely.
     Miru only reads emails from the addresses above.
  ```
- "Go back" link if they want to edit

### 4. Mobile-Friendly Experience
- Stack sections vertically (already responsive)
- Increase button height on mobile: `48px` minimum (thumb-friendly)
- Collapsible "Why Gmail?" explanations (hide long warnings behind disclosure)
- Sticky footer CTA on mobile (button follows scroll)

### 5. Clear CTAs
- Primary button text clarity:
  - Step 1: `"Next: Tell me about your child"`
  - Step 2: `"Next: School details"`
  - Step 3: `"Confirm & connect Gmail"`
- Secondary links: `"Back"` and `"Cancel"`
- Disable next button until required fields valid (real-time validation)

---

## PHASE 2: DISPLAY (school_settings.html)

### 1. School Cards with Visual Identity
**Replace current text-based profile layout:**

Each school card shows:
```
┌─────────────────────────────────┐
│ 🏫 Greenway Academy      [✏️]   │  (emoji + school name + edit icon)
│                                 │
│ Child: Olivia            [→]    │  (linked data with icon → expand)
│ Year: 4A                        │
│ Teacher: Mr Akhurst             │
│ ─────────────────────────────   │
│ Status: ✅ Syncing…             │  (badge)
│ Last synced: 2 mins ago         │
│ [🔄 Sync Now] [⚙️ Options]      │  (action buttons)
│ ─────────────────────────────   │
│ Emails monitored (2):           │
│ • admin@greenway.sch.uk         │
│ • newsletter@greenway.sch.uk    │
│ + Add email                     │
│                                 │
│ [Edit] [Remove school]          │
└─────────────────────────────────┘
```

**Visual enhancements:**
- Card background: white with subtle emoji "watermark" (opacity 0.05)
- School color coding: assign per school (Supabase update optional, or CSS variable from emoji)
- Rounded corners: `16px` (modern look)
- Box-shadow: `0 2px 12px rgba(0,0,0,0.06)` on hover

### 2. Status Badges
Replace vague text with semantic badges:

| Status | Badge | Color | Meaning |
|--------|-------|-------|---------|
| **Connected** | `✅ Connected` | Green (#d1fae5) | Gmail access active, emails syncing |
| **Syncing** | `⏳ Checking inbox…` | Blue (#e0f2fe) | Live email scan in progress |
| **Needs Attention** | `⚠️ Gmail expired` | Red (#fee2e2) | Token error — reconnect needed |
| **Empty** | `—` | Gray | No emails configured yet |

```css
.status-badge {
  display: inline-flex; align-items: center; gap: 4px;
  padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: 600;
}
.status-badge.connected { background: #d1fae5; color: #065f46; }
.status-badge.syncing { background: #e0f2fe; color: #0369a1; }
.status-badge.error { background: #fee2e2; color: #991b1b; }
```

### 3. Last Synced Timestamp + Manual Sync Button
**Add to each card:**
```
Last synced: 2 mins ago [Sync now ↻]
```
- Show "Just now" if < 1 min ago
- Show "2 hours ago" if > 1 hour
- "Never" if no sync yet (empty state)
- Click "Sync now" → button shows `⏳ Checking…` for 3–5 seconds, then updates timestamp
- Sync state persists in browser (store `last_synced_timestamp` in card JS state)

### 4. Connected Emails Preview
**Instead of full list:**
```
Emails monitored (2):
• admin@greenway.sch.uk
• newsletter@greenway.sch.uk
```
- Show first 2 addresses in card
- If 3+: `"+ 1 more email"` → expand or modal
- Quick delete: hover over email → show red X button
- Add new: input + "Add" button stays visible

### 5. Quick Actions
**Card footer buttons:**
- `[✏️ Edit school]` → inline edit mode (school name, address, teacher)
- `[🔄 Sync now]` → manual trigger (already above)
- `[⚙️ Options]` → dropdown with:
  - "View all emails"
  - "Edit emails"
  - "Remove school"
  - "Manage child details"
  
*Alternative (simpler):* Just `[Edit] [Remove]` links at bottom

### 6. Empty State with Onboarding Nudge
**When no schools added:**
```
┌────────────────────────────────────────┐
│  No schools set up yet                 │
│                                        │
│  "Let's add your child's school and   │
│   start getting important emails."    │
│                                        │
│  [+ Add a school] [Learn more]        │
│                                        │
│  💡 Tip: You'll need a Gmail inbox    │
│     to use this feature.              │
└────────────────────────────────────────┘
```
- `[+ Add a school]` → redirect to `/school/signup`
- Illustration or emoji (🏫) for visual interest

---

## PHASE 3: ENHANCEMENTS

### 1. Toast Notifications
**Show temporary feedback on actions:**
```javascript
showToast("✅ Email added — Miru will monitor it", { type: "success", duration: 3000 });
showToast("⏳ Syncing emails…", { type: "info" });
showToast("❌ Couldn't remove — try again", { type: "error" });
```
- Position: bottom-right on desktop, bottom-center on mobile
- Auto-dismiss after 3–4 seconds
- Stack multiple (max 2 visible)
- Accessibility: `role="status"` for screen readers

### 2. Loading States (Skeleton Loaders)
**For async operations:**
- School card fetch: show gray skeleton with shimmer animation
- Email list: placeholder lines instead of empty state during first load
- Sync button: disable + show spinner during fetch

```css
.skeleton {
  background: linear-gradient(90deg, #f3f4f6 25%, #e5e7eb 50%, #f3f4f6 75%);
  background-size: 200% 100%;
  animation: shimmer 2s infinite;
}
@keyframes shimmer {
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}
```

### 3. Error Recovery
**Sync failed?**
- Show error badge: `⚠️ Sync failed`
- Suggest: `[Retry] [View error]`
- Retry button: auto-retry with exponential backoff
- Clear cache: if persistent, offer `[Clear local cache]`

**Gmail token expired?**
- Status: `⚠️ Gmail access expired`
- CTA: `[Reconnect Gmail]` → OAuth flow

### 4. Mobile Responsive
- Card width: `100%` on mobile (no max-width constraint)
- Button stacking: `flex-direction: column` on `< 520px`
- Tab layout: single column instead of sidebar
- Thumb-friendly: all buttons `≥ 44px` height

### 5. Accessibility
**ARIA + semantic HTML:**
```html
<div class="school-card" role="region" aria-label="Greenway Academy">
  <h2>🏫 Greenway Academy</h2>
  <div role="status" aria-live="polite" class="status-badge">✅ Connected</div>
  <label for="email-list-1">Emails monitored:</label>
  <ul id="email-list-1" aria-label="School email addresses">
    <li>admin@greenway.sch.uk</li>
  </ul>
  <button aria-label="Sync emails for Greenway Academy now">🔄 Sync now</button>
</div>
```
- Color-blind safe: badges use icons + text, not color alone
- Keyboard nav: Tab through cards → buttons → links
- Focus visible: outline on all interactive elements

---

## PRIORITIZATION ROADMAP

### Week 1: Visual Polish + Cards Layout
- Convert `school_settings.html` to card-based layout
- Add emoji school icons + color coding (CSS variables)
- Update status badge styling

### Week 2: Step Indicator + Progress Feedback
- Add 3-step progress to `school_signup.html`
- Build visual confirmation screen before Gmail redirect
- Mobile progress indicator

### Week 3: Status Indicators + Sync UX
- Timestamp display + manual sync button
- Toast notifications (add lightweight library or vanilla)
- Loading skeleton states

### Week 4+: Polish & Edge Cases
- Error recovery (retry, reconnect flows)
- Mobile refinements
- ARIA labels + keyboard nav testing
- Empty state design

---

## Implementation Notes

1. **No backend changes needed** for Phase 1–2; all UI/UX improvements
2. **CSS-first:** Use CSS Grid for card layouts, flexbox for components
3. **JavaScript:** Keep vanilla JS (no new frameworks); reuse existing patterns
4. **Storage:** Session/localStorage for last_synced_timestamp, sync state
5. **Testing checklist:**
   - ✓ Click through all 3 steps, verify progress bar updates
   - ✓ Add/remove emails, watch toast notifications
   - ✓ Click "Sync now," watch spinner + timestamp update
   - ✓ Gmail error flow → see reconnect UI
   - ✓ Mobile: cards stack, buttons large, no horizontal scroll
   - ✓ Keyboard nav: Tab through entire flow, can submit with Enter
   - ✓ Screen reader: status messages read aloud

---

## Visual Style Reference
- **Colors:** Existing amber (#f59e0b), green (#10b981), red (#ef4444), gray (#6b7280)
- **Typography:** System font stack, 15px base, 13px labels
- **Spacing:** 16px, 24px, 32px (8px grid)
- **Shadows:** Subtle (0 2px 8px rgba(0,0,0,0.04))
- **Radius:** 12px buttons, 16px cards, 10px inputs

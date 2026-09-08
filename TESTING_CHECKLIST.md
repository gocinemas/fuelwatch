# Miru Testing Checklist

**Purpose:** Catch bugs BEFORE production. Every feature must pass this before deploy.

---

## **Pre-Deploy Checklist (Do This Every Time)**

### **1. Endpoint Testing** ✓
- [ ] API endpoint exists (curl test)
- [ ] Returns correct response format
- [ ] Error handling works (400/401/500)
- [ ] Data actually saved to database

### **2. Frontend Integration** ✓
- [ ] Form/button exists and is visible
- [ ] Click handler is wired up
- [ ] Form validation works
- [ ] Success/error messages show
- [ ] Data persists after page refresh
- [ ] No JavaScript console errors

### **3. End-to-End Flow** ✓
- [ ] User fills form
- [ ] User clicks Save
- [ ] Data appears in database
- [ ] Data loads on next page visit
- [ ] Data displays correctly in UI

### **4. Edge Cases** ✓
- [ ] Empty fields handled
- [ ] Special characters in text
- [ ] Duplicate entries rejected
- [ ] Permissions/auth checked
- [ ] Mobile view works

### **5. Deployment** ✓
- [ ] Code pushed to GitHub
- [ ] Railway redeploy confirmed
- [ ] Tested on production URL (not localhost)
- [ ] Rollback plan ready if needed

---

## **Recent Failures (Lessons Learned)**

| Feature | Issue | Cause | Fix |
|---------|-------|-------|-----|
| Goals API | Endpoint returns 404 | Table didn't exist (migration didn't run) | Run migrations manually |
| Goals UI | Button doesn't work | Frontend save handler not wired | Add JavaScript handler |
| Commute | Values don't save | Frontend handler missing | This turn's fix |
| Algolia | Search slow | Env vars not set | Added to Railway |
| WhatsApp OAuth | Button missing | UI not added to template | Added to school_settings.html |

---

## **Testing Template (Copy & Paste)**

```
FEATURE: [Name]
DATE: [Today]
STATUS: [Ready/Testing/Failed/Deployed]

ENDPOINT TESTS:
- [ ] GET /api/[endpoint] → 200
- [ ] POST /api/[endpoint] → 201
- [ ] Data in DB: SELECT * FROM [table]

FRONTEND TESTS:
- [ ] Button visible
- [ ] Click works (console clear)
- [ ] Form submits
- [ ] Success message shows
- [ ] Refresh → data persists

DEPLOYED:
- [ ] Git push ✓
- [ ] Railway redeploy ✓
- [ ] Production test ✓
- [ ] User can use it ✓
```

---

## **Going Forward**

**Before EVERY feature deployment:**
1. Fill out testing checklist above
2. Run curl tests on endpoints
3. Test frontend manually (3 min)
4. Verify database has data
5. Only then push to production

**If any checkbox fails → STOP and fix it**

This prevents 90% of "it doesn't work" bugs.

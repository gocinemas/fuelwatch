# WhatsApp Business API — Application Checklist

**Goal:** Get approved to read all your WhatsApp messages  
**Timeline:** 2-4 weeks  
**Cost:** $0.05/message (~£5-10/month typical usage)

---

## ✅ Pre-Application Checklist

Before you apply, gather these:

- [ ] Business/Legal name (yours or your business)
- [ ] Website URL (or can use Miru URL)
- [ ] Business phone number
- [ ] Business email
- [ ] WhatsApp phone number you want to connect
- [ ] Clear description of what you'll do with the API

---

## 🎯 Step-by-Step Application

### Step 1: Create Meta Business Account (5 min)

1. Go to **https://business.facebook.com**
2. Click **Create Account**
3. Fill in:
   - Business name: Your name or "Miru Personal"
   - Business email: Your email
   - Business phone: Your phone
   - Country: United Kingdom
4. Click **Next**

### Step 2: Add Your WhatsApp Number (5 min)

1. In Meta Business dashboard → **Accounts** → **WhatsApp**
2. Click **Add WhatsApp Business Account**
3. Enter your WhatsApp phone number (the one you use for school groups)
4. Verify the number via SMS code

### Step 3: Apply for WhatsApp Business API (10 min)

1. Go to **https://developers.facebook.com**
2. Sign in with your Meta Business account
3. Click **Create App** → **Business**
4. App name: "Miru Personal Message Processor"
5. App purpose: "Extract and organize WhatsApp messages to TODOs"
6. Click **Create App**

### Step 4: Add WhatsApp Product (5 min)

1. In your app dashboard → **Products** → **Add Product**
2. Search for **WhatsApp**
3. Click **Set Up**
4. Select your WhatsApp Business Account (from Step 2)

### Step 5: Fill Application Form (15 min)

1. Go to **WhatsApp** product → **Configuration**
2. Under **Application Settings**, fill:

**Use Case Description:**
```
I want to extract actionable information from my personal WhatsApp messages 
and school group chats. The bot will:

1. Read incoming WhatsApp messages
2. Use Claude AI to extract:
   - Action items (TODOs with deadlines)
   - Event dates and times
   - Payment deadlines
   - School announcements
   
3. Organize by category:
   - School: payment deadlines, event dates, pickup times
   - Personal: reminders, plans, commitments
   - Actions: clear action items with due dates

This helps me stay organized with school commitments and personal tasks.

Data will be stored securely in a private database (Supabase) and used 
only for personal task management. No data is shared with third parties.
```

**Business Model:**
```
Personal use - extracting my own messages to improve task organization
```

**Data Usage:**
```
- Messages: Read only (not modified or shared)
- Storage: Private database, deleted after 30 days
- Compliance: GDPR/UK GDPR compliant
```

**Privacy Policy:**
```
I will not share, sell, or use messages for any purpose other than 
personal task extraction. Messages are stored securely and users 
(just me) can request deletion anytime.
```

### Step 6: Submit for Approval (2 min)

1. Review all information
2. Click **Submit for Review**
3. Meta will send confirmation email

---

## ⏱️ What Happens Next

### Days 1-3: Initial Review
- Meta receives your application
- Basic compliance check
- Check for obvious policy violations

### Days 4-14: Detailed Review
- Meta reviews your use case
- May ask follow-up questions
- Check business legitimacy

### Days 15-28: Final Decision
- Approved ✅ OR
- Needs more info 📧 OR
- Rejected ❌ (rare for personal use)

### If Asked Questions
- Check your Meta notifications
- Reply promptly with clear answers
- Emphasize: personal use only, secure storage, GDPR compliant

---

## 📋 Sample Answers to Common Questions

**Q: Will you sell user data?**
> No, this is for personal use only. I will not share, sell, or use messages 
> for marketing/commercial purposes.

**Q: How will you store messages?**
> Messages are stored securely in a private Supabase database. Automatically 
> deleted after 30 days. Only I have access.

**Q: Why do you need this API?**
> To automatically extract actionable items from WhatsApp messages (school 
> deadlines, event dates, TODOs) and organize them in one place.

**Q: Will you use it for marketing/spam?**
> No, strictly personal use for task organization and school communication.

---

## 🎉 After Approval

Once approved, you'll get:

1. **App ID** — Like TWILIO_ACCOUNT_SID
2. **App Secret** — Like TWILIO_AUTH_TOKEN  
3. **Access Token** — To authenticate API calls
4. **Phone Number ID** — Your WhatsApp number identifier
5. **Business Account ID** — Your WhatsApp business account ID

### Next Steps After Approval

1. Store these in Railway env vars:
   ```
   WHATSAPP_BUSINESS_APP_ID=xxxxx
   WHATSAPP_BUSINESS_APP_SECRET=xxxxx
   WHATSAPP_BUSINESS_ACCESS_TOKEN=xxxxx
   WHATSAPP_BUSINESS_PHONE_ID=xxxxx
   ```

2. I'll connect the WhatsApp Business API integration
3. Start reading your messages automatically
4. TODOs will be extracted and organized

---

## 🚨 Troubleshooting

**"Application Rejected"**
- Usually means use case wasn't clear
- Reply to Meta's email with more details
- Emphasize: personal use, secure storage, no data sharing

**"Needs More Information"**
- Meta will send specific questions
- Answer promptly and thoroughly
- Be clear about security/privacy

**"Still Waiting After 4 Weeks"**
- Contact Meta support
- Check app dashboard for any messages
- Be patient (sometimes takes up to 8 weeks)

---

## ✨ What Happens When Connected

Your WhatsApp workflow:

```
School group chat
├─ "Payment due Friday £24.50"
│  ↓ (automatically received via API)
│  ↓ Claude extracts: payment, deadline, amount
│  ↓ Routes to school_comms
│  └─ Miru shows: 🔴 Outstanding payment £24.50 due Friday

Personal message
├─ "Pick up milk tomorrow"
│  ↓ (automatically received via API)
│  ↓ Claude extracts: TODO, due date
│  ↓ Routes to personal TODOs
│  └─ Miru shows: ✓ Pick up milk (tomorrow)

Event announcement
├─ "Sports day June 15th, pickup 3pm"
│  ↓ (automatically received via API)
│  ↓ Claude extracts: event, date, time, pickup location
│  ↓ Routes to calendar
│  └─ Miru shows: 📅 Sports day Jun 15, pickup 3pm
```

---

## 📞 Support

If you get stuck:
- Check **https://developers.facebook.com/docs/whatsapp/cloud-api/**
- Meta support: **https://www.facebook.com/help/**
- Email me once approved so I can set up the integration

---

**Status:** 📋 Ready to apply  
**Your next action:** Follow steps 1-6 above  
**Timeline:** 2-4 weeks to approval  
**Then:** I'll build the integration while you wait

Good luck! 🚀

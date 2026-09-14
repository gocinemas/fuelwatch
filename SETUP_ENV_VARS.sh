#!/bin/bash
# Helper script to set up environment variables for Personal WhatsApp Bot
# You'll need to:
# 1. Gather the values (see instructions below)
# 2. Add them to Railway dashboard manually OR
# 3. Set them locally for testing

echo "╔════════════════════════════════════════════════════════════════════════════╗"
echo "║   Personal WhatsApp Bot — Environment Variables Setup                     ║"
echo "╚════════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "📋 REQUIRED ENVIRONMENT VARIABLES (8 total)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Check which vars are missing
echo "🔍 Checking current environment..."
echo ""

MISSING=()
FOUND=()

if [ -z "$SUPABASE_URL" ]; then
    MISSING+=("SUPABASE_URL")
    echo "  ❌ SUPABASE_URL not set"
else
    FOUND+=("SUPABASE_URL")
    echo "  ✅ SUPABASE_URL already set"
fi

if [ -z "$SUPABASE_KEY" ]; then
    MISSING+=("SUPABASE_KEY")
    echo "  ❌ SUPABASE_KEY not set"
else
    FOUND+=("SUPABASE_KEY")
    echo "  ✅ SUPABASE_KEY already set"
fi

if [ -z "$TWILIO_ACCOUNT_SID" ]; then
    MISSING+=("TWILIO_ACCOUNT_SID")
    echo "  ❌ TWILIO_ACCOUNT_SID not set"
else
    FOUND+=("TWILIO_ACCOUNT_SID")
    echo "  ✅ TWILIO_ACCOUNT_SID already set"
fi

if [ -z "$TWILIO_AUTH_TOKEN" ]; then
    MISSING+=("TWILIO_AUTH_TOKEN")
    echo "  ❌ TWILIO_AUTH_TOKEN not set"
else
    FOUND+=("TWILIO_AUTH_TOKEN")
    echo "  ✅ TWILIO_AUTH_TOKEN already set"
fi

if [ -z "$TWILIO_WHATSAPP_NUMBER" ]; then
    MISSING+=("TWILIO_WHATSAPP_NUMBER")
    echo "  ❌ TWILIO_WHATSAPP_NUMBER not set"
else
    FOUND+=("TWILIO_WHATSAPP_NUMBER")
    echo "  ✅ TWILIO_WHATSAPP_NUMBER already set"
fi

if [ -z "$ANTHROPIC_API_KEY" ]; then
    MISSING+=("ANTHROPIC_API_KEY")
    echo "  ❌ ANTHROPIC_API_KEY not set"
else
    FOUND+=("ANTHROPIC_API_KEY")
    echo "  ✅ ANTHROPIC_API_KEY already set"
fi

if [ -z "$PERSONAL_API_TOKEN" ]; then
    MISSING+=("PERSONAL_API_TOKEN")
    echo "  ❌ PERSONAL_API_TOKEN not set"
else
    FOUND+=("PERSONAL_API_TOKEN")
    echo "  ✅ PERSONAL_API_TOKEN already set"
fi

if [ -z "$DIGEST_CRON_TOKEN" ]; then
    MISSING+=("DIGEST_CRON_TOKEN")
    echo "  ❌ DIGEST_CRON_TOKEN not set"
else
    FOUND+=("DIGEST_CRON_TOKEN")
    echo "  ✅ DIGEST_CRON_TOKEN already set"
fi

echo ""
echo "📊 Summary:"
echo "   ✅ Found: ${#FOUND[@]}/8"
echo "   ❌ Missing: ${#MISSING[@]}/8"
echo ""

if [ ${#MISSING[@]} -eq 0 ]; then
    echo "🎉 All variables are set! You're ready to test."
    echo ""
    echo "Next steps:"
    echo "  1. Run: ./test_personal_bot.sh"
    echo "  2. Send a WhatsApp message to your Twilio number"
    echo "  3. Check Supabase dashboard for TODOs"
else
    echo "⚠️  Missing ${#MISSING[@]} variable(s). You need to set them in Railway:"
    echo ""
    echo "   Missing: ${MISSING[*]}"
    echo ""
    echo "📝 INSTRUCTIONS:"
    echo "   1. Go to Railway Dashboard → Your Project → Variables"
    echo "   2. Add each missing variable (see details below)"
    echo "   3. Railway will redeploy automatically"
    echo ""
fi

echo ""
echo "═════════════════════════════════════════════════════════════════════════════"
echo "WHERE TO GET EACH VALUE:"
echo "═════════════════════════════════════════════════════════════════════════════"
echo ""
echo "1️⃣  SUPABASE_URL"
echo "   → Go to https://app.supabase.com"
echo "   → Select your project → Settings → API"
echo "   → Copy 'Project URL'"
echo ""
echo "2️⃣  SUPABASE_KEY"
echo "   → Same page as above"
echo "   → Copy 'Service Role Secret' (NOT the anon key!)"
echo ""
echo "3️⃣  TWILIO_ACCOUNT_SID"
echo "   → Go to https://console.twilio.com"
echo "   → Copy 'Account SID' from dashboard"
echo ""
echo "4️⃣  TWILIO_AUTH_TOKEN"
echo "   → Same page as above"
echo "   → Copy 'Auth Token'"
echo ""
echo "5️⃣  TWILIO_WHATSAPP_NUMBER"
echo "   → Go to console.twilio.com → Messaging → Try it out → Send WhatsApp"
echo "   → Click on your WhatsApp sender number"
echo "   → Copy the phone number (e.g., +1234567890)"
echo ""
echo "6️⃣  ANTHROPIC_API_KEY"
echo "   → Go to https://console.anthropic.com"
echo "   → Settings → API Keys → Create Key"
echo "   → Copy the key (starts with 'sk-')"
echo ""
echo "7️⃣  PERSONAL_API_TOKEN"
echo "   → Generate a random secret:"
echo "   → Run: openssl rand -hex 32"
echo ""
echo "8️⃣  DIGEST_CRON_TOKEN"
echo "   → Generate another random secret:"
echo "   → Run: openssl rand -hex 32"
echo ""
echo "═════════════════════════════════════════════════════════════════════════════"
echo "HOW TO ADD TO RAILWAY:"
echo "═════════════════════════════════════════════════════════════════════════════"
echo ""
echo "  1. Open https://railway.app → Your Miru/FuelWatch project"
echo "  2. Click 'Variables' tab"
echo "  3. For each missing variable:"
echo "     - Click 'New Variable'"
echo "     - Key: [variable name from above]"
echo "     - Value: [paste the value you copied]"
echo "  4. Click 'Deploy'"
echo "  5. Wait 1-2 minutes for redeploy"
echo ""
echo "═════════════════════════════════════════════════════════════════════════════"

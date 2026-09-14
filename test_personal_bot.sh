#!/bin/bash
# Quick test script for Personal WhatsApp Assistant Bot API

set -e

# Configuration
API_URL="${1:-http://localhost:5000}"
API_TOKEN="${2:-your-secret-token-here}"

echo "🤖 Personal Bot API Test Suite"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "API URL: $API_URL"
echo ""

# Helper function for API calls
api_call() {
    local method=$1
    local endpoint=$2
    local data=$3

    if [ -z "$data" ]; then
        curl -s -X "$method" \
            -H "Authorization: Bearer $API_TOKEN" \
            "$API_URL$endpoint"
    else
        curl -s -X "$method" \
            -H "Authorization: Bearer $API_TOKEN" \
            -H "Content-Type: application/json" \
            -d "$data" \
            "$API_URL$endpoint"
    fi
}

# Test 1: List TODOs
echo "📋 Test 1: List all TODOs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━"
api_call GET "/api/todos" | python3 -m json.tool 2>/dev/null || echo "No TODOs yet"
echo ""

# Test 2: List open TODOs only
echo "✅ Test 2: List open TODOs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━"
api_call GET "/api/todos?status=open" | python3 -m json.tool 2>/dev/null || echo "No open TODOs"
echo ""

# Test 3: List high priority TODOs
echo "🔴 Test 3: List high priority TODOs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
api_call GET "/api/todos?priority=high" | python3 -m json.tool 2>/dev/null || echo "No high priority TODOs"
echo ""

# Test 4: List messages (pagination)
echo "💬 Test 4: List recent messages"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
api_call GET "/api/messages?limit=5" | python3 -m json.tool 2>/dev/null || echo "No messages yet"
echo ""

# Test 5: Get daily summary
echo "📊 Test 5: Get daily summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━"
api_call GET "/api/summaries/daily" | python3 -m json.tool 2>/dev/null || echo "No summary yet"
echo ""

# Test 6: If there's a TODO ID, try updating it
echo "🔧 Test 6: Update a TODO (example)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Usage: update_todo <todo_id>"
echo "Example update payload:"
cat <<EOF | python3 -m json.tool
{
  "status": "done",
  "priority": "high",
  "due_date": "2026-09-20"
}
EOF
echo ""

# Test 7: Health check (no auth needed)
echo "❤️ Test 7: Health check"
echo "━━━━━━━━━━━━━━━━━━━━"
curl -s "$API_URL/health" | python3 -m json.tool 2>/dev/null || echo "Health endpoint not available"
echo ""

echo "✨ Test suite complete!"
echo ""
echo "📝 Notes:"
echo "- Replace 'your-secret-token-here' with your actual PERSONAL_API_TOKEN"
echo "- TODOs are created automatically when WhatsApp messages arrive"
echo "- Test by sending a WhatsApp message like: 'Pick up milk tomorrow'"
echo ""
echo "🚀 Next steps:"
echo "1. Send a WhatsApp message to your Twilio number"
echo "2. Run this script again to see extracted TODOs"
echo "3. Try updating a TODO status to 'done'"

"""
Brief Analyzer — Intelligent priority detection for Miru brief.
Analyzes facts + patterns to determine what matters TODAY.
"""
import json
import re
from typing import Dict, List, Any

def analyze_brief_priorities(facts: List[str], context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze brief facts and context to determine:
    - What's the priority today? (school, commute, weather, calendar, spend)
    - What's unusual? (spend spike, pattern break, missed event)
    - What's actionable? (time-sensitive, needs prep)
    - What's the mood? (busy, relaxed, rushed, normal)

    Returns: {
        "priority_order": ["school", "weather", "commute", ...],
        "anomalies": ["spend_spike: +47%", "high_cafe_visits: 3x"],
        "time_sensitive": ["rain by 3pm", "train in 12min"],
        "mood": "busy|normal|relaxed|rushed",
        "key_action": "bring umbrella for school pickup",
        "summary": "School + rain + high spend"
    }
    """
    priority_order = []
    anomalies = []
    time_sensitive = []
    mood = "normal"
    key_action = ""

    # Extract facts by type
    school_facts = [f for f in facts if any(x in f.lower() for x in ["school", "inaaya", "riaan", "pickup", "violin"])]
    weather_facts = [f for f in facts if any(x in f.lower() for x in ["rain", "snow", "wind", "temp", "°c", "sunny"])]
    commute_facts = [f for f in facts if any(x in f.lower() for x in ["train", "leave", "commute", "drive", "bus"])]
    spend_facts = [f for f in facts if any(x in f.lower() for x in ["spend", "coffee", "visited", "£"])]
    calendar_facts = [f for f in facts if any(x in f.lower() for x in ["meeting", "event", "appointment", "call"])]

    # === PRIORITY RANKING ===
    if school_facts:
        priority_order.append("school")

    if weather_facts and any(x in " ".join(weather_facts).lower() for x in ["rain", "snow", "cold", "hot"]):
        priority_order.append("weather")

    if commute_facts and any(x in " ".join(commute_facts).lower() for x in ["train", "leave", "min"]):
        priority_order.append("commute")

    if calendar_facts:
        priority_order.append("calendar")

    if spend_facts:
        priority_order.append("spend")

    # === ANOMALIES ===
    # Spend spike detection
    spend_text = " ".join(spend_facts)
    if "47%" in spend_text or "50%" in spend_text or "spike" in spend_text.lower():
        anomalies.append("spend_spike")
    if "3x" in spend_text or "4x" in spend_text or "high" in spend_text.lower():
        anomalies.append("frequent_visits")

    # Weather alerts
    weather_text = " ".join(weather_facts)
    if "rain" in weather_text.lower() and "70%" in weather_text:
        time_sensitive.append("rain_by_afternoon")
    if "cold" in weather_text.lower() or "°c" in weather_text and any(x in weather_text for x in ["0", "1", "2", "-"]):
        time_sensitive.append("freezing")

    # Commute alerts
    commute_text = " ".join(commute_facts)
    if "min" in commute_text.lower():
        time_sensitive.append("urgent_commute")

    # === MOOD DETECTION ===
    all_facts_text = " ".join(facts).lower()
    event_count = len(calendar_facts) + len(school_facts)

    if event_count >= 4:
        mood = "busy"
    elif event_count == 0 and "weekend" in all_facts_text:
        mood = "relaxed"
    elif any(x in all_facts_text for x in ["min", "leave", "urgent"]):
        mood = "rushed"
    else:
        mood = "normal"

    # === KEY ACTION ===
    if school_facts and weather_facts and "rain" in weather_text.lower():
        key_action = "bring umbrella for school pickup"
    elif commute_facts and "min" in commute_text.lower():
        key_action = "leave soon for train"
    elif spend_facts and "spike" in spend_text.lower():
        key_action = "watch spending, high cafe visits"
    elif calendar_facts:
        key_action = "time-sensitive events today"

    return {
        "priority_order": priority_order,
        "anomalies": anomalies,
        "time_sensitive": time_sensitive,
        "mood": mood,
        "key_action": key_action,
        "summary": " + ".join(priority_order[:3]) if priority_order else "normal day"
    }


def build_smart_prompt(facts: List[str], analysis: Dict[str, Any], context: Dict[str, Any]) -> str:
    """
    Build Groq prompt that uses analysis to write smart brief.
    Instead of just "narrate the facts", it now says "here's what matters, write based on priorities".
    """
    mood_guide = {
        "busy": "busy, packed day — be direct and actionable",
        "rushed": "time-sensitive morning — lead with urgency",
        "relaxed": "calm weekend — warm and conversational",
        "normal": "standard day — balanced tone"
    }

    priorities = analysis.get("priority_order", [])
    mood_desc = mood_guide.get(analysis.get("mood", "normal"), "balanced")
    key_action = analysis.get("key_action", "")
    anomalies = analysis.get("anomalies", [])

    prompt = f"""You are writing a smart, personal daily brief.

Today's context:
- Priorities (order matters): {', '.join(priorities) if priorities else 'normal day'}
- Mood: {mood_desc}
- Key action: {key_action if key_action else 'none urgent'}
- Anomalies: {', '.join(anomalies) if anomalies else 'none'}

Facts to reference:
{chr(10).join(f'- {f}' for f in facts)}

Write a 2-3 sentence brief that:
1. Leads with TOP priority (not what's easiest to mention)
2. Includes 1 actionable recommendation
3. References anomalies if important (spend spike, pattern break)
4. Speaks directly to the user ('you')
5. Is clear, direct, NOT generic

Mood: {mood_desc}
Avoid: generic phrases, invented suggestions, wishy-washy language
Use: specific details, action verbs, clear priorities
"""
    return prompt


if __name__ == "__main__":
    # Test
    test_facts = [
        "🏫 Inaaya: Violin TODAY at 3:15pm",
        "🌧️ Rain 70% by 3pm",
        "☕ You've visited Costa 3 times this week",
        "⛽ Fuel: 153.7p (near you)",
        "📅 No other events today"
    ]

    analysis = analyze_brief_priorities(test_facts, {})
    print("Analysis:", json.dumps(analysis, indent=2))

    prompt = build_smart_prompt(test_facts, analysis, {})
    print("\nSmart Prompt:\n", prompt)

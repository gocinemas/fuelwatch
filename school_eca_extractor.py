"""
ECA (Extra-Curricular Activities) Extraction
Extract club schedules from WhatsApp + email messages using Groq LLM
"""

import json
import re
from typing import Dict, List, Optional
from datetime import datetime

def extract_eca_clubs_from_message(message_text: str, groq_client) -> List[Dict]:
    """
    Extract ECA club schedule from message text using Groq

    Returns list of clubs:
    [
        {
            "club_name": "Robotics",
            "day_of_week": "Wednesday",
            "start_time": "15:15",
            "end_time": "16:15",
            "year_group": "All Years",
            "location": "S8"
        }
    ]
    """

    if not groq_client:
        return []

    # Only process if message looks like it contains club info
    if not _looks_like_eca_message(message_text):
        return []

    prompt = f"""
Extract ECA (Extra-Curricular Activities) club schedule information from this school message.

Return a JSON array of clubs. Each club should have:
- club_name: Name of the activity/club
- day_of_week: Day (Monday, Tuesday, Wednesday, Thursday, Friday) or "Various"
- start_time: Time in HH:MM format (24-hour), e.g. "15:15"
- end_time: Time in HH:MM format (24-hour)
- year_group: Year groups eligible (e.g. "All Years", "Y7-9", "Y10-13")
- location: Room/location if mentioned

IMPORTANT:
- Return ONLY valid JSON array, no other text
- If no clubs found, return empty array: []
- Times must be 24-hour format
- Days must be full names (Monday, Tuesday, etc.) or "Various"
- If multiple clubs mentioned, include all of them

Message:
{message_text}

Return JSON array:
"""

    try:
        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=500
        )

        response_text = response.choices[0].message.content.strip()

        # Extract JSON from response (it might be wrapped in markdown)
        json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
        if json_match:
            clubs = json.loads(json_match.group(0))

            # Validate and sanitize
            validated_clubs = []
            for club in clubs:
                if _is_valid_club(club):
                    validated_clubs.append(club)

            return validated_clubs

        return []

    except json.JSONDecodeError:
        print(f"[ECA] Failed to parse Groq response as JSON")
        return []
    except Exception as e:
        print(f"[ECA] Groq extraction error: {e}")
        return []


def _looks_like_eca_message(text: str) -> bool:
    """Quick check if message likely contains ECA/club info"""
    text_lower = text.lower()

    eca_keywords = [
        'club', 'activity', 'eca', 'signup', 'register', 'register now',
        'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
        '3:15', '15:15', '11:40', '3:45', 'lunch', 'after school',
        'year 7', 'year 8', 'y7', 'y8', 'y9', 'y10',
        'rehearsal', 'practice', 'team', 'sports', 'music', 'drama',
        'chess', 'robotics', 'coding', 'football', 'netball', 'badminton'
    ]

    return any(keyword in text_lower for keyword in eca_keywords)


def _is_valid_club(club: Dict) -> bool:
    """Validate club data structure"""
    required_fields = ['club_name', 'day_of_week', 'start_time', 'end_time', 'year_group']

    # Check all required fields present and non-empty
    if not all(field in club and club[field] for field in required_fields):
        return False

    # Validate time format (HH:MM)
    time_pattern = r'^\d{2}:\d{2}$'
    if not re.match(time_pattern, str(club.get('start_time', ''))):
        return False
    if not re.match(time_pattern, str(club.get('end_time', ''))):
        return False

    # Validate day of week
    valid_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Various']
    if club.get('day_of_week') not in valid_days:
        return False

    # Validate year group (should contain Y or "All")
    year_group = club.get('year_group', '').lower()
    if not any(x in year_group for x in ['y', 'all', 'year']):
        return False

    return True


def deduplicate_clubs(clubs: List[Dict], existing_clubs: List[Dict]) -> List[Dict]:
    """
    Filter out clubs that already exist in database
    Two clubs are same if: club_name + day_of_week + start_time match
    """
    new_clubs = []

    for club in clubs:
        is_duplicate = False

        for existing in existing_clubs:
            if (club.get('club_name', '').lower() == existing.get('club_name', '').lower() and
                club.get('day_of_week', '').lower() == existing.get('day_of_week', '').lower() and
                club.get('start_time') == existing.get('start_time')):
                is_duplicate = True
                break

        if not is_duplicate:
            new_clubs.append(club)

    return new_clubs

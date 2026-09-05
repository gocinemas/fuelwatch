"""
School Comms NLP: Categorize WhatsApp + email messages
Pattern-based extraction (no LLM needed for speed + cost)
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# ============================================================================
# MESSAGE CATEGORIZATION
# ============================================================================

class SchoolMessageExtractor:
    """
    Extract school event details from natural language
    Returns: (category, confidence, extracted_date, action_text)

    Categories:
    - event: School event (date + time)
    - action-needed: Requires parent action (form, payment, consent)
    - permission-slip: Needs signature/return
    - fyi: Informational (no action)
    - announcement: School-wide announcement
    """

    # Event keywords + urgency levels
    EVENT_KEYWORDS = {
        'trip', 'excursion', 'outing', 'visit', 'event', 'sports day', 'sports',
        'assembly', 'concert', 'performance', 'show', 'production', 'play',
        'half-term', 'holiday', 'break', 'inset', 'training day',
        'nativity', 'carol', 'christmas', 'easter', 'summer', 'show',
        'workshop', 'session', 'class', 'swimming', 'pe', 'games', 'match'
    }

    ACTION_KEYWORDS = {
        'forms', 'form', 'fill', 'return', 'submit', 'pay', 'payment', 'balance',
        'kit', 'uniform', 'pe kit', 'permission', 'consent', 'confirm',
        'consent form', 'reply', 'response', 'action', 'complete', 'book',
        'register', 'sign up', 'deadline', 'due', 'urgent'
    }

    PERMISSION_KEYWORDS = {
        'permission slip', 'permission form', 'signed', 'sign', 'signature',
        'return by', 'bring back', 'needs signing', 'parental consent',
        'parental permission', 'consent slip'
    }

    ANNOUNCEMENT_KEYWORDS = {
        'reminder', 'update', 'notice', 'please', 'note', 'information',
        'attention', 'alert', 'message', 'important'
    }

    DATE_PATTERNS = [
        # "Friday 5th September" or "Friday 5 September"
        (r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)?\s*(\d{1,2})(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*', 'dmy'),
        # "September 5" or "Sep 5"
        (r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})', 'mdy'),
        # "5/9", "05/09"
        (r'(\d{1,2})/(\d{1,2})', 'dmy_slash'),
        # "next Friday", "this Tuesday"
        (r'(?:next|this)\s+(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)', 'relative'),
        # "tomorrow", "next week"
        (r'(?:tomorrow|next week|next month)', 'relative'),
    ]

    TIME_PATTERNS = [
        (r'(\d{1,2}):(\d{2})\s*(?:am|AM|pm|PM)', 'time_ampm'),
        (r'at\s+(\d{1,2}):(\d{2})', 'time_at'),
        (r'(\d{1,2})(?:am|AM|pm|PM)', 'time_short'),
    ]

    @classmethod
    def categorize(cls, text: str) -> Tuple[str, float, str, str]:
        """
        Categorize message and extract key details
        Returns: (category, confidence, extracted_date, action_text)
        """
        text_lower = text.lower()
        scores = {
            'event': 0,
            'action-needed': 0,
            'permission-slip': 0,
            'announcement': 0,
            'fyi': 0
        }

        # Score each category
        scores['event'] = cls._score_keywords(text_lower, cls.EVENT_KEYWORDS)
        scores['action-needed'] = cls._score_keywords(text_lower, cls.ACTION_KEYWORDS)
        scores['permission-slip'] = cls._score_keywords(text_lower, cls.PERMISSION_KEYWORDS) * 1.5  # Higher weight
        scores['announcement'] = cls._score_keywords(text_lower, cls.ANNOUNCEMENT_KEYWORDS)

        # Determine primary category
        category = max(scores, key=scores.get) if max(scores.values()) > 0 else 'fyi'
        confidence = scores[category]

        # Extract date + action
        extracted_date = cls._extract_date(text)
        action_text = cls._extract_action(text, category)

        # Normalize confidence to 0-1
        confidence = min(confidence / 10, 1.0)

        return (category, confidence, extracted_date, action_text)

    @classmethod
    def _score_keywords(cls, text: str, keywords: set) -> float:
        """Score text for keyword matches"""
        score = 0
        for keyword in keywords:
            if keyword in text:
                # Bonus for exact phrase matches
                score += 2 if f' {keyword} ' in f' {text} ' else 1
        return score

    @classmethod
    def _extract_date(cls, text: str) -> str:
        """Extract date from text (returns ISO format YYYY-MM-DD or empty)"""
        text_lower = text.lower()

        # Look for specific date patterns
        for pattern, pattern_type in cls.DATE_PATTERNS:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    if pattern_type == 'relative':
                        # Handle relative dates
                        matched = match.group(0).lower()
                        if 'tomorrow' in matched:
                            return (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
                        elif 'next week' in matched:
                            return (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')
                        elif 'next' in matched:
                            # Extract day of week
                            for day_offset in range(1, 8):
                                day_name = (datetime.now() + timedelta(days=day_offset)).strftime('%A').lower()
                                if day_name in matched:
                                    return (datetime.now() + timedelta(days=day_offset)).strftime('%Y-%m-%d')
                    else:
                        # Parse specific dates (simplified)
                        # This is complex; for now return empty if can't fully parse
                        pass
                except:
                    pass

        return ''

    @classmethod
    def _extract_action(cls, text: str, category: str) -> str:
        """
        Extract the required action from text
        Examples: "bring PE kit", "return by Friday", "pay £15"
        """
        text_lower = text.lower()

        # Look for payment amounts
        payment_match = re.search(r'£([\d.]+)', text)
        if payment_match:
            return f"pay £{payment_match.group(1)}"

        # Look for deadlines
        if 'return by' in text_lower:
            deadline_match = re.search(r'return by\s+([^,.\n]+)', text, re.IGNORECASE)
            if deadline_match:
                return f"return by {deadline_match.group(1).strip()}"

        # Look for kit/items needed
        kit_match = re.search(r'bring\s+([^,.\n]+?)\s*(?:on|by|for)', text, re.IGNORECASE)
        if kit_match:
            return f"bring {kit_match.group(1).strip()}"

        # Look for permission/forms
        if 'permission' in text_lower or 'form' in text_lower:
            return "complete permission slip"

        # Generic action for action-needed messages
        if category == 'action-needed':
            # Extract first sentence
            first_sentence = re.split(r'[.!?]', text)[0]
            return first_sentence[:50] + "..." if len(first_sentence) > 50 else first_sentence

        return ''

    @classmethod
    def deduplicate_event(cls, message_text: str, existing_events: List[Dict]) -> bool:
        """
        Check if this message describes an already-extracted event
        Returns: True if duplicate, False if new
        """
        msg_lower = message_text.lower()

        for event in existing_events:
            # Simple dedup: match title + date
            title_lower = event.get('title', '').lower()
            event_date = event.get('date', '')

            if title_lower in msg_lower and event_date:
                return True

            # Also check if same keywords appear
            if any(keyword in msg_lower for keyword in [title_lower] * 1):
                return True

        return False


# ============================================================================
# USAGE in school_oauth_handlers.py
# ============================================================================
# from school_message_nlp import SchoolMessageExtractor
#
# def process_whatsapp_message(msg, db):
#     text = msg.get('text', {}).get('body', '')
#     msg_id = msg.get('id')
#
#     # Categorize
#     category, confidence, date_str, action = SchoolMessageExtractor.categorize(text)
#
#     # Update database with NLP results
#     db.table('school_wa_messages').update({
#         'category': category,
#         'confidence': confidence,
#         'extracted_date': date_str,
#         'extracted_action': action
#     }).eq('wa_message_id', msg_id).execute()

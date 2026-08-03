"""
Global constants for Miru platform.
UK-centric configuration: currency, timezone, date formats, etc.
"""

import zoneinfo
from datetime import datetime

# ── CURRENCY ────────────────────────────────────────────────────────────────
CURRENCY_SYMBOL = "£"
CURRENCY_CODE = "GBP"

# ── TIMEZONE & DATES ────────────────────────────────────────────────────────
TIMEZONE = zoneinfo.ZoneInfo("Europe/London")

def now_london():
    """Current time in London timezone."""
    return datetime.now(TIMEZONE)

def today_london():
    """Current date in London timezone."""
    return now_london().date()

# Date format: DD/MM/YY (British standard)
DATE_FORMAT_DISPLAY = "%d/%m/%y"  # 24/07/26
DATE_FORMAT_LONG = "%A, %d %B %Y"  # Monday, 24 July 2026
DATE_FORMAT_ISO = "%Y-%m-%d"  # 2026-07-24 (for storage)

# ── LOCATION ────────────────────────────────────────────────────────────────
COUNTRY = "UK"
COUNTRY_CODE = "GB"

# ── TIME RANGES ─────────────────────────────────────────────────────────────
EARLY_MORNING = (5, 9)      # 05:00-09:00 — commute time
MORNING = (9, 12)            # 09:00-12:00
LUNCH = (12, 14)             # 12:00-14:00
AFTERNOON = (14, 17)         # 14:00-17:00
EVENING = (17, 21)           # 17:00-21:00
NIGHT = (21, 24)             # 21:00-00:00
LATE_NIGHT = (0, 5)          # 00:00-05:00

# ── SPEND CATEGORIES ────────────────────────────────────────────────────────
SPEND_CATEGORIES = {
    "Groceries": {"icon": "🛒", "priority": 1},
    "Coffee & Lunch": {"icon": "☕", "priority": 2},
    "Dining": {"icon": "🍽️", "priority": 3},
    "Takeaway": {"icon": "🥡", "priority": 4},
    "Fuel": {"icon": "⛽", "priority": 5},
    "Parking": {"icon": "🅿️", "priority": 6},
    "Transport": {"icon": "🚌", "priority": 7},
    "Shopping": {"icon": "🛍️", "priority": 8},
    "Entertainment": {"icon": "🎬", "priority": 9},
    "Health": {"icon": "💊", "priority": 10},
    "Other": {"icon": "📌", "priority": 99},
}

# ── SAVING CATEGORIES ───────────────────────────────────────────────────────
SAVE_CATEGORIES = ["books", "shows", "articles", "music", "places"]

# ── THRESHOLDS FOR ALERTS ───────────────────────────────────────────────────
DAILY_SPEND_WARNING = 50.0          # Alert if daily spend > £50
WEEKLY_SPEND_WARNING = 200.0        # Alert if weekly spend > £200
CAFE_VISIT_FREQUENCY = 3            # Alert if > 3 cafe visits/week
UNUSUAL_MERCHANT = 100.0            # Alert if single transaction > £100

# ── WEATHER CONFIG ──────────────────────────────────────────────────────────
WEATHER_API = "open-meteo"  # Free, no API key needed
WEATHER_UNITS = "celsius"
WEATHER_TIMEZONE = "Europe/London"

# ── COMMUTE DEFAULTS ────────────────────────────────────────────────────────
DEFAULT_COMMUTE_RADIUS_KM = 5
TRAIN_BUFFER_MINUTES = 5  # Show trains departing in next N minutes

# ── SCHOOL INFO ─────────────────────────────────────────────────────────────
SCHOOL_POLL_TOKEN = "miru-digest-2026"
SCHOOL_POLL_INTERVAL_HOURS = 6  # Poll every 6 hours
SCHOOL_COMMS = {
    "Stanns Heath": {
        "email": "stannsheathjuniors-surrey@scopay.com",
        "url": "https://www.stannsheath.surrey.sch.uk",
    },
    "New Haw": {
        "email": "office@new-haw.surrey.sch.uk",
        "url": "https://www.new-haw.surrey.sch.uk",
    },
}

# ── API DEFAULTS ────────────────────────────────────────────────────────────
API_TIMEOUT_SECONDS = 10
API_RETRY_COUNT = 3
API_RETRY_DELAY_SECONDS = 2

# ── DISPLAY SETTINGS ────────────────────────────────────────────────────────
DECIMAL_PLACES_CURRENCY = 2
MAX_RECENT_ITEMS = 5
MAX_TOP_ITEMS = 3

# ── MORNING BRIEF ───────────────────────────────────────────────────────────
MORNING_BRIEF_DEFAULT_TIME = "07:30"  # HH:MM, 24-hour format
MORNING_BRIEF_MIN_TIME = "05:00"
MORNING_BRIEF_MAX_TIME = "22:00"
MORNING_BRIEF_TIME_STEP_MINUTES = 15
MORNING_BRIEF_CRON = "*/5 * * * *"  # Run every 5 minutes to check scheduled times

# Brief category toggles
MORNING_BRIEF_CATEGORIES = [
    "weather",
    "trains",
    "school",
    "spend",
    "calendar",
    "deliveries",
    "bin_day",
]

# Default prefs for new users
MORNING_BRIEF_DEFAULT_PREFS = {
    "enabled": False,
    "time": MORNING_BRIEF_DEFAULT_TIME,
    "timezone": "Europe/London",
    "opt_out_categories": [],
}

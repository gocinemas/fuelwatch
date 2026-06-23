-- Migration: Context Memory System (Phase 1)
-- Creates user_saves_v2 and save_interactions tables

-- Table 1: User Saves (Core persistence layer)
CREATE TABLE IF NOT EXISTS user_saves_v2 (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  user_phone TEXT NOT NULL,

  -- Content
  title TEXT NOT NULL,
  description TEXT,
  url TEXT,
  category TEXT,  -- "dining", "service", "event", "place", "receipt", etc.
  source TEXT,    -- "brief", "whatsapp", "web", "email", "calendar"

  -- Location context
  location TEXT,  -- Address or postcode
  lat FLOAT8, lng FLOAT8,  -- Coordinates if available

  -- Metadata
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  last_surfaced_at TIMESTAMP WITH TIME ZONE,
  user_tags TEXT[],  -- User-added tags as array
  ai_tags TEXT[],    -- Auto-generated tags as array

  -- User feedback loop
  user_notes TEXT,   -- Annotation/personal notes
  relevance_score FLOAT8 DEFAULT 0.5,  -- Updated by pattern detection (0-1)
  visit_count INT DEFAULT 0,  -- How many times user acted on this save

  -- Status
  archived BOOLEAN DEFAULT false,
  deleted_at TIMESTAMP WITH TIME ZONE,  -- Soft delete

  -- Indexing
  CONSTRAINT user_phone_not_empty CHECK (user_phone != ''),
  CONSTRAINT title_not_empty CHECK (title != ''),
  CONSTRAINT relevance_valid CHECK (relevance_score >= 0 AND relevance_score <= 1)
);

-- Create indexes for fast querying
CREATE INDEX idx_user_saves_v2_phone_active
  ON user_saves_v2(user_phone, archived, deleted_at)
  WHERE deleted_at IS NULL AND archived = false;

CREATE INDEX idx_user_saves_v2_category
  ON user_saves_v2(user_phone, category);

CREATE INDEX idx_user_saves_v2_created_at
  ON user_saves_v2(user_phone, created_at DESC);

CREATE INDEX idx_user_saves_v2_location
  ON user_saves_v2(user_phone, location);

CREATE INDEX idx_user_saves_v2_title_fulltext
  ON user_saves_v2 USING GIN(to_tsvector('english', title || ' ' || COALESCE(description, '')));


-- Table 2: Save Interactions (For pattern detection & analytics)
CREATE TABLE IF NOT EXISTS save_interactions (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  save_id BIGINT NOT NULL REFERENCES user_saves_v2(id) ON DELETE CASCADE,
  user_phone TEXT NOT NULL,

  event_type TEXT NOT NULL,  -- "surfaced", "clicked", "visited", "feedback", "archived"
  context JSONB,             -- JSON with brief_id, location, day_of_week, etc.
  timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

  -- For surfacing: did user act on it?
  action_taken BOOLEAN,      -- User clicked/visited after surfacing
  latency_mins INT            -- Minutes between surfacing and action
);

-- Create indexes for analytics
CREATE INDEX idx_save_interactions_save_id
  ON save_interactions(save_id, event_type);

CREATE INDEX idx_save_interactions_user_phone
  ON save_interactions(user_phone, timestamp DESC);

CREATE INDEX idx_save_interactions_event_type
  ON save_interactions(event_type);


-- Table 3: User Patterns (For pattern detection)
CREATE TABLE IF NOT EXISTS user_save_patterns (
  id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  user_phone TEXT NOT NULL UNIQUE,

  patterns JSONB,  -- {
               --   "high_save_categories": ["dining", "service"],
               --   "save_frequency": 0.5,  // per day
               --   "optimal_surfacing_times": [7, 8, 12, 18],
               --   "last_updated": "2026-06-23T..."
               -- }

  last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

  CONSTRAINT user_phone_not_empty CHECK (user_phone != '')
);

CREATE INDEX idx_user_save_patterns_phone
  ON user_save_patterns(user_phone);


-- Seed some categories (optional, for dropdown hints)
CREATE TABLE IF NOT EXISTS save_categories (
  id INT PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  emoji TEXT,
  description TEXT
);

INSERT INTO save_categories (id, name, emoji, description) VALUES
  (1, 'dining', '🍽️', 'Restaurants, cafes, takeaways'),
  (2, 'service', '🔧', 'Plumbers, mechanics, services'),
  (3, 'place', '📍', 'Shops, venues, landmarks'),
  (4, 'event', '📅', 'Calendar events, performances'),
  (5, 'receipt', '🧾', 'Receipts, expenses, shopping'),
  (6, 'health', '⚕️', 'Doctors, dentists, clinics'),
  (7, 'fitness', '💪', 'Gyms, yoga, sports'),
  (8, 'education', '📚', 'Schools, courses, learning'),
  (9, 'entertainment', '🎬', 'Cinemas, theaters, activities'),
  (10, 'other', '📌', 'Everything else')
ON CONFLICT DO NOTHING;

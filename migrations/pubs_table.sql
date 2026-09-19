-- Pubs database with FHRS + OSM + confidence tiers
CREATE TABLE IF NOT EXISTS pubs (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  postcode TEXT,
  lat FLOAT,
  lon FLOAT,

  -- FHRS (Food Standards Agency)
  fhrs_id TEXT UNIQUE,
  fhrs_rating INTEGER,  -- 5=very good, 4=good, 3=satisfactory, 2=improvement, 1=poor, 0=awaiting inspection

  -- OSM (OpenStreetMap)
  osm_id TEXT UNIQUE,

  -- Confidence scoring
  confidence_tier TEXT CHECK (confidence_tier IN ('VERIFIED', 'LIKELY', 'UNVERIFIED')),
  match_confidence FLOAT,  -- 0-1 score for FHRS→OSM match

  -- Metadata
  data JSONB,  -- Full record backup
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),

  -- Indexes for performance
  CONSTRAINT postcode_or_coords CHECK (postcode IS NOT NULL OR (lat IS NOT NULL AND lon IS NOT NULL))
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_pubs_postcode ON pubs(postcode);
CREATE INDEX IF NOT EXISTS idx_pubs_confidence ON pubs(confidence_tier);
CREATE INDEX IF NOT EXISTS idx_pubs_latlon ON pubs(lat, lon);
CREATE INDEX IF NOT EXISTS idx_pubs_fhrs_id ON pubs(fhrs_id);
CREATE INDEX IF NOT EXISTS idx_pubs_osm_id ON pubs(osm_id);

-- Search nearby pubs by lat/lon
-- SELECT name, distance_km FROM pubs
-- WHERE SQRT(POW(lat - ?, 2) + POW(lon - ?, 2)) * 111 < 5
-- ORDER BY distance_km LIMIT 5;

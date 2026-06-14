-- NRAA database schema
-- Reverse-engineered from import_results.py, import_shooters.py, and app.py queries.
-- Adds explicit target_max (5|6) to strings so 60-pt → 50-pt conversion is data-driven,
-- not a hardcoded (state, year) lookup.

CREATE TABLE IF NOT EXISTS states (
    state_id   SERIAL PRIMARY KEY,
    code       VARCHAR(8)  UNIQUE NOT NULL,
    name       VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS clubs (
    club_id    SERIAL PRIMARY KEY,
    club_name  VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS shooters (
    sid         INTEGER PRIMARY KEY,
    last_name   VARCHAR(100),
    first_name  VARCHAR(100),
    pref_name   VARCHAR(100),
    club_id     INTEGER REFERENCES clubs(club_id)
);

CREATE INDEX IF NOT EXISTS idx_shooters_name ON shooters (LOWER(first_name), LOWER(last_name));
CREATE INDEX IF NOT EXISTS idx_shooters_pref ON shooters (LOWER(pref_name), LOWER(last_name));

CREATE TABLE IF NOT EXISTS competitions (
    competition_id SERIAL PRIMARY KEY,
    state_id       INTEGER NOT NULL REFERENCES states(state_id),
    year           INTEGER NOT NULL,
    name           VARCHAR(255),
    UNIQUE (state_id, year)
);

CREATE TABLE IF NOT EXISTS aggregates (
    aggregate_id    SERIAL PRIMARY KEY,
    competition_id  INTEGER NOT NULL REFERENCES competitions(competition_id),
    match_number    INTEGER,
    match_name      VARCHAR(255),
    discipline      VARCHAR(100),
    place           INTEGER,
    shooter_sid     INTEGER REFERENCES shooters(sid),
    state           VARCHAR(8),
    info            VARCHAR(255),
    score           NUMERIC(8,3)
);

CREATE INDEX IF NOT EXISTS idx_agg_comp ON aggregates (competition_id);
CREATE INDEX IF NOT EXISTS idx_agg_shooter ON aggregates (shooter_sid);
CREATE INDEX IF NOT EXISTS idx_agg_discipline ON aggregates (discipline);

CREATE TABLE IF NOT EXISTS strings (
    string_id        SERIAL PRIMARY KEY,
    competition_id   INTEGER NOT NULL REFERENCES competitions(competition_id),
    match_number     INTEGER,
    match_name       VARCHAR(255),
    distance         INTEGER,
    distance_unit    VARCHAR(8),
    discipline       VARCHAR(100),
    place            INTEGER,
    shooter_sid      INTEGER REFERENCES shooters(sid),
    state            VARCHAR(8),
    shots_raw        TEXT,
    info             VARCHAR(255),
    score            NUMERIC(8,3),
    is_kings_queens  BOOLEAN NOT NULL DEFAULT FALSE,
    target_max       SMALLINT NOT NULL DEFAULT 5
        CHECK (target_max IN (5, 6))
);

CREATE INDEX IF NOT EXISTS idx_str_comp ON strings (competition_id);
CREATE INDEX IF NOT EXISTS idx_str_shooter ON strings (shooter_sid);
CREATE INDEX IF NOT EXISTS idx_str_discipline ON strings (discipline);
CREATE INDEX IF NOT EXISTS idx_str_kings ON strings (is_kings_queens) WHERE is_kings_queens = TRUE;

CREATE TABLE IF NOT EXISTS shots (
    shot_id     SERIAL PRIMARY KEY,
    string_id   INTEGER NOT NULL REFERENCES strings(string_id) ON DELETE CASCADE,
    shot_number INTEGER NOT NULL,
    shot_value  VARCHAR(2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shots_string ON shots (string_id);
CREATE INDEX IF NOT EXISTS idx_shots_value ON shots (shot_value);

CREATE TABLE IF NOT EXISTS unmatched_results (
    unmatched_id  SERIAL PRIMARY KEY,
    competition   VARCHAR(255),
    match_number  INTEGER,
    match_name    VARCHAR(255),
    distance      INTEGER,
    distance_unit VARCHAR(8),
    discipline    VARCHAR(100),
    place         INTEGER,
    full_name     VARCHAR(255),
    last_name     VARCHAR(100),
    first_name    VARCHAR(100),
    club          VARCHAR(255),
    state         VARCHAR(8),
    shots         TEXT,
    info          VARCHAR(255),
    score         NUMERIC(8,3),
    is_aggregate  BOOLEAN
);

-- Seed the standard NRAA state codes so import scripts can FK to them.
INSERT INTO states (code, name) VALUES
    ('NRAA', 'National Rifle Association of Australia'),
    ('VRA',  'Victorian Rifle Association'),
    ('NSWRA','New South Wales Rifle Association'),
    ('QRA',  'Queensland Rifle Association'),
    ('NQRA', 'North Queensland Rifle Association'),
    ('SARA', 'South Australian Rifle Association'),
    ('WARA', 'Western Australian Rifle Association'),
    ('TRA',  'Tasmanian Rifle Association'),
    ('NTRA', 'Northern Territory Rifle Association'),
    ('ACTRA','ACT Rifle Association')
ON CONFLICT (code) DO NOTHING;

-- Survey schema: crowdsourced ranking of K&Q top-score comparisons.
-- Survey_sets are pre-populated; responses/rankings are filled by users.
-- All display data lives on survey_set_items so the survey can run
-- on a Fly Postgres without access to the source strings table.

CREATE TABLE IF NOT EXISTS survey_sets (
    set_id          SERIAL PRIMARY KEY,
    competition_id  INTEGER,
    competition_label VARCHAR(255),
    match_number    INTEGER,
    match_name      VARCHAR(255),
    distance        INTEGER,
    distance_unit   VARCHAR(8),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS survey_set_items (
    item_id     SERIAL PRIMARY KEY,
    set_id      INTEGER NOT NULL REFERENCES survey_sets(set_id) ON DELETE CASCADE,
    category    VARCHAR(16) NOT NULL,    -- 'TR-A','FO','F-STD','FTR','Sporter'
    discipline  VARCHAR(64) NOT NULL,    -- raw discipline label shown to user
    distance    INTEGER,
    distance_unit VARCHAR(8),
    score       NUMERIC(8,3) NOT NULL,
    string_id   INTEGER,                  -- reference to source string (local only)
    position    SMALLINT NOT NULL         -- stable order within set
);

CREATE INDEX IF NOT EXISTS idx_survey_items_set ON survey_set_items (set_id);

CREATE TABLE IF NOT EXISTS survey_responses (
    response_id   SERIAL PRIMARY KEY,
    token         VARCHAR(40) UNIQUE NOT NULL,
    sid           INTEGER,
    sid_name      VARCHAR(255),
    email         VARCHAR(255),
    set_ids       INTEGER[] NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at  TIMESTAMPTZ,
    user_agent    TEXT,
    ip_hash       VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_survey_responses_sid ON survey_responses (sid);

CREATE TABLE IF NOT EXISTS survey_rankings (
    ranking_id   SERIAL PRIMARY KEY,
    response_id  INTEGER NOT NULL REFERENCES survey_responses(response_id) ON DELETE CASCADE,
    set_id       INTEGER NOT NULL,
    item_id      INTEGER NOT NULL,
    rank         SMALLINT NOT NULL,        -- 1 = best, N = worst
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (response_id, set_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_survey_rankings_set ON survey_rankings (set_id);

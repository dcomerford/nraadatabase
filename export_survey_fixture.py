"""Export survey fixture data (schema + shooters + comparison sets) to a single
SQL file that can be loaded into the Fly Postgres for the survey app.

Run:
    python3 export_survey_fixture.py -o survey_fixture.sql

Then load on Fly:
    fly postgres connect -a <pg-app-name> < survey_fixture.sql

The fixture is self-contained: it creates the minimum tables needed
(shooters + survey_*) so the Fly DB does not need to import the full
nraadb dataset.
"""
import argparse

from db import get_connection

MIN_SCHEMA = """
DROP TABLE IF EXISTS survey_rankings CASCADE;
DROP TABLE IF EXISTS survey_responses CASCADE;
DROP TABLE IF EXISTS survey_set_items CASCADE;
DROP TABLE IF EXISTS survey_sets CASCADE;
DROP TABLE IF EXISTS shooters CASCADE;

CREATE TABLE shooters (
    sid         INTEGER PRIMARY KEY,
    last_name   VARCHAR(100),
    first_name  VARCHAR(100),
    pref_name   VARCHAR(100)
);

CREATE TABLE survey_sets (
    set_id            SERIAL PRIMARY KEY,
    competition_id    INTEGER,
    competition_label VARCHAR(255),
    match_number      INTEGER,
    match_name        VARCHAR(255),
    distance          INTEGER,
    distance_unit     VARCHAR(8),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE survey_set_items (
    item_id       SERIAL PRIMARY KEY,
    set_id        INTEGER NOT NULL REFERENCES survey_sets(set_id) ON DELETE CASCADE,
    category      VARCHAR(16) NOT NULL,
    discipline    VARCHAR(64) NOT NULL,
    distance      INTEGER,
    distance_unit VARCHAR(8),
    score         NUMERIC(8,3) NOT NULL,
    string_id     INTEGER,
    position      SMALLINT NOT NULL,
    centres       INTEGER,
    adrian_v3     NUMERIC(8,3),
    peter_score   NUMERIC(8,3)
);
CREATE INDEX idx_survey_items_set ON survey_set_items (set_id);

CREATE TABLE survey_responses (
    response_id  SERIAL PRIMARY KEY,
    token        VARCHAR(40) UNIQUE NOT NULL,
    sid          INTEGER,
    sid_name     VARCHAR(255),
    email        VARCHAR(255),
    set_ids      INTEGER[] NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    user_agent   TEXT,
    ip_hash      VARCHAR(64)
);
CREATE INDEX idx_survey_responses_sid ON survey_responses (sid);

CREATE TABLE survey_rankings (
    ranking_id   SERIAL PRIMARY KEY,
    response_id  INTEGER NOT NULL REFERENCES survey_responses(response_id) ON DELETE CASCADE,
    set_id       INTEGER NOT NULL,
    item_id      INTEGER NOT NULL,
    rank         SMALLINT NOT NULL,
    adrian_rank  SMALLINT,        -- the rank Adrian's formula gave this item
    peter_rank   SMALLINT,        -- the rank Peter's (June 13) formula gave this item
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (response_id, set_id, item_id)
);
CREATE INDEX idx_survey_rankings_set ON survey_rankings (set_id);
"""


def quote_sql_literal(v):
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return 'TRUE' if v else 'FALSE'
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("'", "''")
    return f"'{s}'"


def dump_table(cur, out, table, columns, where='TRUE'):
    cur.execute(f"SELECT {', '.join(columns)} FROM {table} WHERE {where}")
    rows = cur.fetchall()
    if not rows:
        return 0
    out.write(f"\n-- {table}: {len(rows)} rows\n")
    cols = ', '.join(columns)
    for r in rows:
        vals = ', '.join(quote_sql_literal(v) for v in r)
        out.write(f"INSERT INTO {table} ({cols}) VALUES ({vals});\n")
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-o', '--output', default='survey_fixture.sql')
    args = ap.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    with open(args.output, 'w') as out:
        out.write("-- Auto-generated survey fixture. Load into a fresh Postgres for the survey app.\n")
        out.write("BEGIN;\n")
        out.write(MIN_SCHEMA)
        out.write("\n")

        n_shooters = dump_table(cur, out, 'shooters',
            ['sid', 'last_name', 'first_name', 'pref_name'])

        n_sets = dump_table(cur, out, 'survey_sets',
            ['set_id', 'competition_id', 'competition_label',
             'match_number', 'match_name', 'distance', 'distance_unit'])

        n_items = dump_table(cur, out, 'survey_set_items',
            ['item_id', 'set_id', 'category', 'discipline',
             'distance', 'distance_unit', 'score', 'string_id', 'position',
             'centres', 'adrian_v3', 'peter_score'])

        # Reset sequences past max(id) so SERIAL keeps working
        out.write("\n-- Reset sequences\n")
        out.write("SELECT setval('survey_sets_set_id_seq', "
                  "(SELECT COALESCE(MAX(set_id),1) FROM survey_sets));\n")
        out.write("SELECT setval('survey_set_items_item_id_seq', "
                  "(SELECT COALESCE(MAX(item_id),1) FROM survey_set_items));\n")
        out.write("COMMIT;\n")

    print(f"Wrote {args.output}: {n_shooters} shooters, {n_sets} sets, {n_items} items")


if __name__ == '__main__':
    main()

"""Add v5 columns to survey_set_items and survey_rankings, back-fill existing rows.

Idempotent: uses ADD COLUMN IF NOT EXISTS and only back-fills NULL rows.
Run against whichever DB DATABASE_URL points at (local nraadb or Fly mcsidb).
"""
import os
import psycopg2

URL = os.environ.get('DATABASE_URL') or 'postgres://localhost/nraadb'

# V5 factors / structure mirroring build_survey_sets.py
def v5_factor(category):
    if category in ('F-Open', 'FO'): return 1.406
    if category in ('F-STD', 'FS'):  return 1.475
    if category == 'FTR':            return 1.450
    if category in ('TR-A', 'TR'):   return 1.412
    return 1.383  # Sporter merged

def v5_adjusted(category, raw_score_int, centres):
    is_f = category in ('F-Open', 'F-STD', 'FTR')
    eq = raw_score_int if is_f else raw_score_int * 1.2
    return round((eq + centres * 0.7) * v5_factor(category), 3)


conn = psycopg2.connect(URL)
cur = conn.cursor()

print(f'Connected to: {URL.split("@")[-1] if "@" in URL else URL}')

# 1. Add columns
cur.execute("ALTER TABLE survey_set_items ADD COLUMN IF NOT EXISTS v5_score NUMERIC(8,3)")
cur.execute("ALTER TABLE survey_rankings  ADD COLUMN IF NOT EXISTS v5_rank  SMALLINT")
print('Columns added (or already present)')

# 2. Back-fill v5_score on existing items
cur.execute(
    "SELECT item_id, category, score, COALESCE(centres,0) "
    "FROM survey_set_items WHERE v5_score IS NULL"
)
items = cur.fetchall()
print(f'Back-filling {len(items)} items with v5_score')
for item_id, cat, score, centres in items:
    v5 = v5_adjusted(cat, int(float(score)), int(centres))
    cur.execute("UPDATE survey_set_items SET v5_score=%s WHERE item_id=%s",
                (v5, item_id))

# 3. Back-fill v5_rank on existing rankings (compute per (response, set))
cur.execute("SELECT DISTINCT response_id, set_id FROM survey_rankings WHERE v5_rank IS NULL")
groups = cur.fetchall()
print(f'Back-filling v5_rank for {len(groups)} (response, set) groups')
for response_id, set_id in groups:
    cur.execute(
        "SELECT item_id, v5_score FROM survey_set_items WHERE set_id=%s",
        (set_id,),
    )
    rows = cur.fetchall()
    sorted_rows = sorted(rows, key=lambda r: -(float(r[1]) if r[1] is not None else -1))
    rank_by_item = {r[0]: i+1 for i, r in enumerate(sorted_rows)}
    for item_id, v5_rank in rank_by_item.items():
        cur.execute(
            "UPDATE survey_rankings SET v5_rank=%s "
            "WHERE response_id=%s AND set_id=%s AND item_id=%s",
            (v5_rank, response_id, set_id, item_id),
        )

conn.commit()

cur.execute("SELECT COUNT(*) FROM survey_set_items WHERE v5_score IS NOT NULL")
print(f'survey_set_items with v5_score: {cur.fetchone()[0]}')
cur.execute("SELECT COUNT(*) FROM survey_rankings WHERE v5_rank IS NOT NULL")
print(f'survey_rankings with v5_rank:   {cur.fetchone()[0]}')
cur.close(); conn.close()
print('Done.')

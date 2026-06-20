"""One-shot backup: dump the live survey schema + data from Fly Postgres
into a local SQL file (survey_backup.sql) before tearing the Fly app down.

Usage (with `fly proxy 15432:5432 -a nraa-survey-db` running):
    python3 backup_fly_survey.py
"""
import psycopg2

DB_URL = 'postgres://mcsi_survey:ZwwwaGFxXz4o06c@127.0.0.1:15432/mcsidb?sslmode=disable'
OUT = 'survey_backup.sql'


def q(v):
    if v is None: return 'NULL'
    if isinstance(v, bool): return 'TRUE' if v else 'FALSE'
    if isinstance(v, (int, float)): return str(v)
    if isinstance(v, list):
        return "ARRAY[" + ",".join(q(x) for x in v) + "]::int[]"
    s = str(v).replace("'", "''")
    return f"'{s}'"


def dump(cur, out, table, columns, where='TRUE'):
    cur.execute(f"SELECT {', '.join(columns)} FROM {table} WHERE {where}")
    rows = cur.fetchall()
    if not rows: return 0
    out.write(f"\n-- {table}: {len(rows)} rows\n")
    cols = ', '.join(columns)
    for r in rows:
        vals = ', '.join(q(v) for v in r)
        out.write(f"INSERT INTO {table} ({cols}) VALUES ({vals});\n")
    return len(rows)


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    with open(OUT, 'w') as out:
        out.write("-- Fly survey backup\n-- Restore with: psql ... < survey_backup.sql\n")
        out.write("BEGIN;\n")
        n1 = dump(cur, out, 'survey_sets',
            ['set_id','competition_id','competition_label','match_number',
             'match_name','distance','distance_unit','created_at'])
        n2 = dump(cur, out, 'survey_set_items',
            ['item_id','set_id','category','discipline','distance','distance_unit',
             'score','string_id','position'])
        n3 = dump(cur, out, 'survey_responses',
            ['response_id','token','sid','sid_name','email','set_ids',
             'created_at','completed_at','user_agent','ip_hash'])
        n4 = dump(cur, out, 'survey_rankings',
            ['ranking_id','response_id','set_id','item_id','rank','submitted_at'])
        out.write("\n-- Reset sequences\n")
        for tbl, col in [
            ('survey_sets','set_id'), ('survey_set_items','item_id'),
            ('survey_responses','response_id'), ('survey_rankings','ranking_id'),
        ]:
            out.write(f"SELECT setval('{tbl}_{col}_seq', "
                      f"(SELECT COALESCE(MAX({col}),1) FROM {tbl}));\n")
        out.write("COMMIT;\n")
    print(f'Wrote {OUT}: sets={n1} items={n2} responses={n3} rankings={n4}')


if __name__ == '__main__':
    main()

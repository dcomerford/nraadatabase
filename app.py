from flask import Flask, render_template, request, jsonify
from db import get_connection

app = Flask(__name__)

# Discipline normalization mapping
DISCIPLINE_MAP = {
    # Target Rifle
    'Target Rifle - A': 'TR-A',
    'Target Rifle - B': 'TR-B',
    'Target Rifle - C': 'TR-C',
    'Target Rifle - C - Tyro': 'TR-C',

    # F Standard
    'F Standard - A': 'F-Std-A',
    'F Standard-A': 'F-Std-A',
    'F Standard - B': 'F-Std-B',
    'F Standard-B': 'F-Std-B',
    'Division F Standard Open': 'F-Std-Open',

    # F Open
    'F Open': 'F-Open',
    'F Open - FO': 'F-Open',
    'Division Open': 'F-Open',

    # F/TR
    'F/TR - FTR': 'FTR',

    # Sporter - Hunter became Sporter Open
    'Sporter - Hunter A': 'Sporter-Open',
    'Sporter - Production Class OPEN - Open': 'Sporter-Open',
    'Sporter - F Class Open - A': 'Sporter-Open',

    # Sporter PC
    'Sporter - Production Class - Sporter PC': 'Sporter-PC',
    'Sporter - F Class - A': 'Sporter-PC',
}

DISCIPLINE_GROUPS = {
    'Target Rifle': ['TR-A', 'TR-B', 'TR-C'],
    'F Standard': ['F-Std-A', 'F-Std-B', 'F-Std-Open'],
    'F Open': ['F-Open'],
    'F/TR': ['FTR'],
    'Sporter': ['Sporter-Open', 'Sporter-PC'],
}


def normalize_discipline(disc):
    """Normalize discipline name."""
    return DISCIPLINE_MAP.get(disc, disc)


def get_db():
    return get_connection()


@app.route('/')
def index():
    """Home page with state/year selection."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute('''
        SELECT s.state_id, s.code, s.name,
               array_agg(DISTINCT c.year ORDER BY c.year DESC) as years
        FROM states s
        JOIN competitions c ON c.state_id = s.state_id
        GROUP BY s.state_id, s.code, s.name
        ORDER BY s.code;
    ''')

    states = []
    for state_id, code, name, years in cur.fetchall():
        states.append({
            'id': state_id,
            'code': code,
            'name': name,
            'years': years
        })

    conn.close()
    return render_template('index.html', states=states)


@app.route('/competition/<state_code>/<int:year>')
def competition(state_code, year):
    """Show competition details."""
    conn = get_db()
    cur = conn.cursor()

    # Get competition
    cur.execute('''
        SELECT c.competition_id, s.code, s.name, c.year
        FROM competitions c
        JOIN states s ON c.state_id = s.state_id
        WHERE s.code = %s AND c.year = %s;
    ''', (state_code, year))

    comp = cur.fetchone()
    if not comp:
        return "Competition not found", 404

    comp_id, code, state_name, year = comp

    # Get unique match names (aggregate types)
    cur.execute('''
        SELECT DISTINCT match_name, match_number
        FROM aggregates
        WHERE competition_id = %s
        ORDER BY match_number;
    ''', (comp_id,))

    aggregates = [{'name': row[0], 'number': row[1]} for row in cur.fetchall()]

    # Get unique distances/ranges
    cur.execute('''
        SELECT DISTINCT distance, distance_unit, match_name
        FROM strings
        WHERE competition_id = %s AND distance IS NOT NULL
        ORDER BY distance;
    ''', (comp_id,))

    ranges = [{'distance': row[0], 'unit': row[1], 'match': row[2]} for row in cur.fetchall()]

    # Get disciplines
    cur.execute('''
        SELECT DISTINCT discipline FROM aggregates WHERE competition_id = %s
        UNION
        SELECT DISTINCT discipline FROM strings WHERE competition_id = %s
        ORDER BY discipline;
    ''', (comp_id, comp_id))

    disciplines = []
    for row in cur.fetchall():
        disc = row[0]
        normalized = normalize_discipline(disc)
        disciplines.append({'original': disc, 'normalized': normalized})

    conn.close()

    return render_template('competition.html',
                           state_code=code,
                           state_name=state_name,
                           year=year,
                           comp_id=comp_id,
                           aggregates=aggregates,
                           ranges=ranges,
                           disciplines=disciplines)


@app.route('/aggregate/<int:comp_id>/<path:match_name>')
def aggregate_results(comp_id, match_name):
    """Show aggregate results."""
    conn = get_db()
    cur = conn.cursor()

    # Get competition info
    cur.execute('''
        SELECT s.code, c.year FROM competitions c
        JOIN states s ON c.state_id = s.state_id
        WHERE c.competition_id = %s;
    ''', (comp_id,))
    comp_info = cur.fetchone()

    # Get results grouped by discipline
    cur.execute('''
        SELECT a.discipline, a.place, sh.sid, sh.first_name, sh.last_name,
               cl.club_name, a.state, a.info, a.score
        FROM aggregates a
        JOIN shooters sh ON a.shooter_sid = sh.sid
        LEFT JOIN clubs cl ON sh.club_id = cl.club_id
        WHERE a.competition_id = %s AND a.match_name = %s
        ORDER BY a.discipline, a.place;
    ''', (comp_id, match_name))

    results_by_discipline = {}
    for row in cur.fetchall():
        disc = row[0]
        normalized = normalize_discipline(disc)
        if normalized not in results_by_discipline:
            results_by_discipline[normalized] = []
        results_by_discipline[normalized].append({
            'place': row[1],
            'sid': row[2],
            'first_name': row[3],
            'last_name': row[4],
            'club': row[5],
            'state': row[6],
            'info': row[7],
            'score': row[8]
        })

    conn.close()

    return render_template('aggregate.html',
                           comp_info=comp_info,
                           match_name=match_name,
                           results=results_by_discipline)


@app.route('/shooter/<int:sid>')
def shooter_profile(sid):
    """Show shooter profile and history."""
    conn = get_db()
    cur = conn.cursor()

    # Get shooter info
    cur.execute('''
        SELECT sh.sid, sh.first_name, sh.last_name, sh.pref_name, cl.club_name
        FROM shooters sh
        LEFT JOIN clubs cl ON sh.club_id = cl.club_id
        WHERE sh.sid = %s;
    ''', (sid,))

    shooter = cur.fetchone()
    if not shooter:
        return "Shooter not found", 404

    shooter_info = {
        'sid': shooter[0],
        'first_name': shooter[1],
        'last_name': shooter[2],
        'pref_name': shooter[3],
        'club': shooter[4]
    }

    # Get aggregate results
    cur.execute('''
        SELECT s.code, c.year, a.match_name, a.discipline, a.place, a.score
        FROM aggregates a
        JOIN competitions c ON a.competition_id = c.competition_id
        JOIN states s ON c.state_id = s.state_id
        WHERE a.shooter_sid = %s
        ORDER BY c.year DESC, s.code, a.match_name;
    ''', (sid,))

    aggregates = []
    for row in cur.fetchall():
        aggregates.append({
            'state': row[0],
            'year': row[1],
            'match': row[2],
            'discipline': normalize_discipline(row[3]),
            'place': row[4],
            'score': row[5]
        })

    # Get shot statistics
    cur.execute('''
        SELECT shot_value, COUNT(*) as cnt
        FROM shots sh
        JOIN strings st ON sh.string_id = st.string_id
        WHERE st.shooter_sid = %s
        GROUP BY shot_value
        ORDER BY shot_value;
    ''', (sid,))

    shot_stats = {row[0]: row[1] for row in cur.fetchall()}

    conn.close()

    return render_template('shooter.html',
                           shooter=shooter_info,
                           aggregates=aggregates,
                           shot_stats=shot_stats)


@app.route('/reports')
def reports():
    """Reports page."""
    return render_template('reports.html')


@app.route('/api/report/top-shooters')
def report_top_shooters():
    """Top shooters by discipline across years."""
    conn = get_db()
    cur = conn.cursor()

    discipline = request.args.get('discipline', 'TR-A')

    # Map normalized discipline back to originals
    original_discs = [k for k, v in DISCIPLINE_MAP.items() if v == discipline]
    if not original_discs:
        original_discs = [discipline]

    placeholders = ','.join(['%s'] * len(original_discs))

    cur.execute(f'''
        SELECT sh.sid, sh.first_name, sh.last_name, cl.club_name,
               COUNT(CASE WHEN a.place = 1 THEN 1 END) as wins,
               COUNT(CASE WHEN a.place <= 3 THEN 1 END) as podiums,
               AVG(a.score) as avg_score,
               COUNT(*) as total_entries
        FROM aggregates a
        JOIN shooters sh ON a.shooter_sid = sh.sid
        LEFT JOIN clubs cl ON sh.club_id = cl.club_id
        WHERE a.discipline IN ({placeholders})
          AND a.match_name LIKE '%Grand%'
        GROUP BY sh.sid, sh.first_name, sh.last_name, cl.club_name
        HAVING COUNT(*) >= 3
        ORDER BY wins DESC, podiums DESC, avg_score DESC
        LIMIT 50;
    ''', original_discs)

    results = []
    for row in cur.fetchall():
        results.append({
            'sid': row[0],
            'name': f"{row[1]} {row[2]}",
            'club': row[3],
            'wins': row[4],
            'podiums': row[5],
            'avg_score': float(row[6]) if row[6] else 0,
            'entries': row[7]
        })

    conn.close()
    return jsonify(results)


@app.route('/api/report/discipline-stats')
def report_discipline_stats():
    """Discipline participation over years."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute('''
        SELECT c.year, a.discipline, COUNT(DISTINCT a.shooter_sid) as shooters
        FROM aggregates a
        JOIN competitions c ON a.competition_id = c.competition_id
        WHERE a.match_name LIKE '%Grand%'
        GROUP BY c.year, a.discipline
        ORDER BY c.year, a.discipline;
    ''')

    results = []
    for row in cur.fetchall():
        results.append({
            'year': row[0],
            'discipline': normalize_discipline(row[1]),
            'shooters': row[2]
        })

    conn.close()
    return jsonify(results)


@app.route('/api/report/shot-distribution')
def report_shot_distribution():
    """Shot value distribution."""
    conn = get_db()
    cur = conn.cursor()

    discipline = request.args.get('discipline', 'TR-A')
    original_discs = [k for k, v in DISCIPLINE_MAP.items() if v == discipline]
    if not original_discs:
        original_discs = [discipline]

    placeholders = ','.join(['%s'] * len(original_discs))

    cur.execute(f'''
        SELECT sh.shot_value, COUNT(*) as cnt
        FROM shots sh
        JOIN strings st ON sh.string_id = st.string_id
        WHERE st.discipline IN ({placeholders})
        GROUP BY sh.shot_value
        ORDER BY sh.shot_value;
    ''', original_discs)

    results = {row[0]: row[1] for row in cur.fetchall()}
    conn.close()
    return jsonify(results)


if __name__ == '__main__':
    import os
    port = int(os.getenv('PORT', 5001))
    debug = os.getenv('FLASK_ENV') != 'production'
    app.run(host='0.0.0.0', port=port, debug=debug)

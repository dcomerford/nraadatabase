import hashlib
import re
import secrets

from flask import Flask, abort, redirect, render_template, request, url_for, jsonify
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

# MCSI (Mixed Category Score Index) parameters for 2025
# Formula: MCSI = ((Score + Centres) × Multiplier) + Offset
MCSI_PARAMS = {
    'F-Open': {'multiplier': 1.42, 'offset': 1.8},
    'F-Std-A': {'multiplier': 1.42, 'offset': 1.8},
    'F-Std-B': {'multiplier': 1.42, 'offset': 1.8},
    'F-Std-Open': {'multiplier': 1.42, 'offset': 1.8},
    'FTR': {'multiplier': 1.42, 'offset': 1.8},
    'TR-A': {'multiplier': 1.62, 'offset': 8.4},
    'TR-B': {'multiplier': 1.62, 'offset': 8.4},
    'TR-C': {'multiplier': 1.62, 'offset': 8.4},
    'Sporter-Open': {'multiplier': 1.5, 'offset': 12},
    'Sporter-PC': {'multiplier': 1.5, 'offset': 12},
}


def convert_60_to_50(score, shots_raw):
    """Convert a score shot on 60-point target to 50-point equivalent.
    X → V (centre), 6 → 5 (max score)
    """
    if score is None:
        return None, None

    if shots_raw:
        # Count 6s to subtract from score
        sixes = shots_raw.count('6')
        # Convert shots: X→V, 6→5
        converted_shots = shots_raw.replace('X', 'V').replace('6', '5')
    else:
        sixes = 0
        converted_shots = shots_raw

    # Subtract 1 point for each 6 (since 6→5)
    points = int(score)
    centres = round((score % 1) * 10)
    converted_points = points - sixes
    converted_score = converted_points + (centres / 10)

    return round(converted_score, 2), converted_shots


def needs_60_to_50_conversion(state_code, year, discipline):
    """Check if this competition/discipline needs 60→50 conversion."""
    # VRA 2025 Sporter disciplines were shot on 60s
    if state_code == 'VRA' and year == 2025:
        norm = normalize_discipline(discipline)
        if 'Sporter' in norm:
            return True
    return False


def calculate_mcsi(score, discipline, state_code=None, year=None, shots_raw=None):
    """Calculate MCSI from score and discipline."""
    if score is None:
        return None

    normalized = normalize_discipline(discipline)
    params = MCSI_PARAMS.get(normalized)

    if not params:
        return None

    # Apply 60→50 conversion if needed
    if state_code and year and needs_60_to_50_conversion(state_code, year, discipline):
        score, _ = convert_60_to_50(score, shots_raw)
        if score is None:
            return None

    points = int(score)
    centres = round((score % 1) * 10)
    mcsi = ((points + centres) * params['multiplier']) + params['offset']
    return round(mcsi, 2)


def normalize_discipline(disc):
    """Normalize discipline name."""
    return DISCIPLINE_MAP.get(disc, disc)


def get_db():
    return get_connection()


@app.route('/')
def index():
    """Home page with state/year selection.

    On the survey-only deployment (SURVEY_ONLY=1) the strings/competitions
    tables don't exist, so redirect to the survey instead of crashing.
    """
    import os
    if os.getenv('SURVEY_ONLY') == '1':
        return redirect(url_for('survey_start'))
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


@app.route('/event/<int:comp_id>/mcsi')
def event_mcsi(comp_id):
    """Event page showing all shooters ranked by MCSI regardless of discipline."""
    conn = get_db()
    cur = conn.cursor()

    # Get competition info
    cur.execute('''
        SELECT c.competition_id, s.code, s.name, c.year
        FROM competitions c
        JOIN states s ON c.state_id = s.state_id
        WHERE c.competition_id = %s;
    ''', (comp_id,))

    comp = cur.fetchone()
    if not comp:
        conn.close()
        return "Competition not found", 404

    comp_id, state_code, state_name, year = comp

    # Get only Kings/Queens strings for this competition (using pre-calculated flag)
    cur.execute('''
        SELECT st.string_id, sh.sid, sh.first_name, sh.last_name, cl.club_name,
               st.discipline, st.distance, st.distance_unit, st.score, st.match_name, st.shots_raw
        FROM strings st
        JOIN shooters sh ON st.shooter_sid = sh.sid
        LEFT JOIN clubs cl ON sh.club_id = cl.club_id
        WHERE st.competition_id = %s AND st.score IS NOT NULL
          AND st.is_kings_queens = TRUE
        ORDER BY sh.last_name, sh.first_name;
    ''', (comp_id,))

    # Calculate MCSI for each string
    all_strings = []
    for row in cur.fetchall():
        string_id, sid, first, last, club, disc, distance, unit, score, match_name, shots_raw = row

        # Check if 60→50 conversion needed
        needs_conversion = needs_60_to_50_conversion(state_code, year, disc)
        display_score = score
        if needs_conversion and shots_raw:
            display_score, _ = convert_60_to_50(score, shots_raw)

        mcsi = calculate_mcsi(score, disc, state_code, year, shots_raw)
        if mcsi:
            all_strings.append({
                'string_id': string_id,
                'sid': sid,
                'name': f"{first} {last}",
                'club': club,
                'discipline': normalize_discipline(disc),
                'distance': f"{distance}{unit}" if distance else '',
                'score': float(score),
                'score_50': float(display_score) if display_score else None,
                'converted': needs_conversion,
                'mcsi': mcsi,
                'match': match_name
            })

    # Sort by MCSI descending
    all_strings.sort(key=lambda x: x['mcsi'], reverse=True)

    # Also aggregate by shooter - best MCSI and average
    shooter_stats = {}
    for s in all_strings:
        sid = s['sid']
        if sid not in shooter_stats:
            shooter_stats[sid] = {
                'sid': sid,
                'name': s['name'],
                'club': s['club'],
                'scores': [],
                'disciplines': set()
            }
        shooter_stats[sid]['scores'].append(s['mcsi'])
        shooter_stats[sid]['disciplines'].add(s['discipline'])

    # Calculate stats
    shooter_list = []
    for sid, data in shooter_stats.items():
        scores = data['scores']
        shooter_list.append({
            'sid': data['sid'],
            'name': data['name'],
            'club': data['club'],
            'total_mcsi': round(sum(scores), 2),
            'avg_mcsi': round(sum(scores) / len(scores), 2),
            'shoots': len(scores),
            'disciplines': sorted(data['disciplines'])
        })

    # Sort by total MCSI descending
    shooter_list.sort(key=lambda x: x['total_mcsi'], reverse=True)

    conn.close()

    return render_template('event_mcsi.html',
                           state_code=state_code,
                           state_name=state_name,
                           year=year,
                           comp_id=comp_id,
                           shooters=shooter_list,
                           top_strings=all_strings[:50])


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


@app.route('/api/report/mcsi-leaderboard')
def report_mcsi_leaderboard():
    """MCSI leaderboard - top shooters across all disciplines."""
    conn = get_db()
    cur = conn.cursor()

    year = request.args.get('year', type=int)

    # Get all string scores with disciplines
    query = '''
        SELECT sh.sid, sh.first_name, sh.last_name, cl.club_name,
               st.discipline, st.score, c.year, s.code, st.shots_raw
        FROM strings st
        JOIN shooters sh ON st.shooter_sid = sh.sid
        LEFT JOIN clubs cl ON sh.club_id = cl.club_id
        JOIN competitions c ON st.competition_id = c.competition_id
        JOIN states s ON c.state_id = s.state_id
        WHERE st.score IS NOT NULL
    '''
    params = []

    if year:
        query += ' AND c.year = %s'
        params.append(year)

    cur.execute(query, params)

    # Calculate MCSI for each score and aggregate by shooter
    shooter_scores = {}
    for sid, first, last, club, disc, score, yr, state_code, shots_raw in cur.fetchall():
        mcsi = calculate_mcsi(score, disc, state_code, yr, shots_raw)
        if mcsi is None:
            continue

        if sid not in shooter_scores:
            shooter_scores[sid] = {
                'sid': sid,
                'name': f"{first} {last}",
                'club': club,
                'scores': [],
                'disciplines': set()
            }
        shooter_scores[sid]['scores'].append(mcsi)
        shooter_scores[sid]['disciplines'].add(normalize_discipline(disc))

    # Calculate averages and sort
    results = []
    for sid, data in shooter_scores.items():
        if len(data['scores']) >= 5:  # Minimum 5 scores
            avg_mcsi = sum(data['scores']) / len(data['scores'])
            top_10_avg = sum(sorted(data['scores'], reverse=True)[:10]) / min(10, len(data['scores']))
            results.append({
                'sid': data['sid'],
                'name': data['name'],
                'club': data['club'],
                'avg_mcsi': round(avg_mcsi, 2),
                'top_10_avg': round(top_10_avg, 2),
                'total_scores': len(data['scores']),
                'disciplines': list(data['disciplines'])
            })

    results.sort(key=lambda x: x['top_10_avg'], reverse=True)
    conn.close()
    return jsonify(results[:100])


@app.route('/api/report/mcsi-comparison')
def report_mcsi_comparison():
    """Compare a shooter's MCSI across disciplines."""
    conn = get_db()
    cur = conn.cursor()

    sid = request.args.get('sid', type=int)
    if not sid:
        return jsonify({'error': 'sid required'}), 400

    cur.execute('''
        SELECT st.discipline, st.score, c.year, s.code
        FROM strings st
        JOIN competitions c ON st.competition_id = c.competition_id
        JOIN states s ON c.state_id = s.state_id
        WHERE st.shooter_sid = %s AND st.score IS NOT NULL
        ORDER BY c.year DESC, st.discipline;
    ''', (sid,))

    by_discipline = {}
    for disc, score, year, state in cur.fetchall():
        mcsi = calculate_mcsi(score, disc)
        if mcsi is None:
            continue

        norm_disc = normalize_discipline(disc)
        if norm_disc not in by_discipline:
            by_discipline[norm_disc] = []
        by_discipline[norm_disc].append({
            'score': float(score),
            'mcsi': mcsi,
            'year': year,
            'state': state
        })

    # Calculate stats per discipline
    results = {}
    for disc, scores in by_discipline.items():
        mcsi_values = [s['mcsi'] for s in scores]
        results[disc] = {
            'count': len(scores),
            'avg_mcsi': round(sum(mcsi_values) / len(mcsi_values), 2),
            'best_mcsi': round(max(mcsi_values), 2),
            'scores': scores[:20]  # Last 20 scores
        }

    conn.close()
    return jsonify(results)


# ---------------------------------------------------------------------------
# Survey: crowdsourced ranking of top K&Q scores across disciplines
# ---------------------------------------------------------------------------

SURVEY_SETS_PER_RESPONSE = 10
EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


def _hash_ip(ip):
    if not ip:
        return None
    return hashlib.sha256(ip.encode('utf-8')).hexdigest()[:32]


@app.route('/survey')
@app.route('/survey/')
def survey_start():
    return render_template('survey_start.html')


@app.route('/survey/start', methods=['POST'])
def survey_begin():
    sid_raw = (request.form.get('sid') or '').strip()
    email = (request.form.get('email') or '').strip() or None

    error = None
    if not sid_raw.isdigit():
        error = 'SID must be a number.'
    if email and not EMAIL_RE.match(email):
        error = 'Email looks invalid.'

    sid = int(sid_raw) if sid_raw.isdigit() else None
    sid_name = None
    if sid is not None and error is None:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(pref_name, first_name), last_name FROM shooters WHERE sid=%s",
            (sid,),
        )
        row = cur.fetchone()
        if not row:
            error = f'SID {sid} not found in NRAA shooter list.'
        else:
            first, last = row
            sid_name = f"{first or ''} {last or ''}".strip() or None
        cur.close()
        conn.close()

    if error:
        return render_template(
            'survey_start.html',
            error=error, sid=sid_raw, email=email or '',
        ), 400

    conn = get_connection()
    cur = conn.cursor()
    # Pick N random sets that have at least 3 items
    cur.execute("""
        SELECT s.set_id FROM survey_sets s
        WHERE (SELECT COUNT(*) FROM survey_set_items i WHERE i.set_id = s.set_id) >= 3
        ORDER BY random() LIMIT %s
    """, (SURVEY_SETS_PER_RESPONSE,))
    set_ids = [r[0] for r in cur.fetchall()]
    if not set_ids:
        cur.close(); conn.close()
        return 'No survey sets configured yet — please try again later.', 503

    token = secrets.token_urlsafe(16)
    ip_hash = _hash_ip(request.headers.get('Fly-Client-IP') or request.remote_addr)
    ua = (request.headers.get('User-Agent') or '')[:512]

    cur.execute(
        """INSERT INTO survey_responses
              (token, sid, sid_name, email, set_ids, user_agent, ip_hash)
           VALUES (%s,%s,%s,%s,%s,%s,%s)""",
        (token, sid, sid_name, email, set_ids, ua, ip_hash),
    )
    conn.commit()
    cur.close()
    conn.close()
    return redirect(url_for('survey_rank', token=token, idx=1))


def _load_response(cur, token):
    cur.execute(
        """SELECT response_id, sid, sid_name, email, set_ids, completed_at
           FROM survey_responses WHERE token=%s""", (token,),
    )
    row = cur.fetchone()
    if not row:
        abort(404)
    return {
        'response_id': row[0], 'sid': row[1], 'sid_name': row[2],
        'email': row[3], 'set_ids': row[4], 'completed_at': row[5],
    }


@app.route('/survey/<token>/<int:idx>', methods=['GET', 'POST'])
def survey_rank(token, idx):
    conn = get_connection()
    cur = conn.cursor()
    resp = _load_response(cur, token)
    total = len(resp['set_ids'])
    if idx < 1 or idx > total:
        cur.close(); conn.close()
        abort(404)
    set_id = resp['set_ids'][idx - 1]

    if request.method == 'POST':
        # Form sends ranked_item_ids = "12,4,7,9,15" (best → worst)
        raw = request.form.get('ranked_item_ids', '')
        try:
            order = [int(x) for x in raw.split(',') if x.strip()]
        except ValueError:
            order = []
        # Pull items (with adrian_v3, peter_score, v5_score) for ranking computation
        cur.execute(
            """SELECT item_id, adrian_v3, peter_score, v5_score
               FROM survey_set_items WHERE set_id=%s""",
            (set_id,),
        )
        items_rows = cur.fetchall()
        valid = {r[0] for r in items_rows}
        if not order or set(order) != valid:
            cur.close(); conn.close()
            return 'Invalid ranking submission — please go back and try again.', 400
        # Compute ranks per formula (highest score → rank 1)
        def ranks_by_score(rows, idx):
            sorted_rows = sorted(rows, key=lambda r: -(float(r[idx]) if r[idx] is not None else -1))
            return {r[0]: i+1 for i, r in enumerate(sorted_rows)}
        adrian_rank_by_item = ranks_by_score(items_rows, 1)
        peter_rank_by_item  = ranks_by_score(items_rows, 2)
        v5_rank_by_item     = ranks_by_score(items_rows, 3)
        # Replace any prior rankings for this (response, set)
        cur.execute(
            "DELETE FROM survey_rankings WHERE response_id=%s AND set_id=%s",
            (resp['response_id'], set_id),
        )
        for rank_pos, item_id in enumerate(order, start=1):
            cur.execute(
                """INSERT INTO survey_rankings
                      (response_id, set_id, item_id, rank, adrian_rank, peter_rank, v5_rank)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (resp['response_id'], set_id, item_id, rank_pos,
                 adrian_rank_by_item.get(item_id),
                 peter_rank_by_item.get(item_id),
                 v5_rank_by_item.get(item_id)),
            )
        conn.commit(); cur.close(); conn.close()
        # Show the feedback page comparing user's order vs Adrian's
        return redirect(url_for('survey_feedback', token=token, idx=idx))

    # GET: fetch the set + items
    cur.execute(
        """SELECT competition_label, match_name, distance, distance_unit
           FROM survey_sets WHERE set_id=%s""", (set_id,),
    )
    sr = cur.fetchone()
    set_info = {
        'competition_label': sr[0], 'match_name': sr[1],
        'distance': sr[2], 'distance_unit': sr[3],
    }
    cur.execute(
        """SELECT item_id, category, discipline, distance, distance_unit, score
           FROM survey_set_items WHERE set_id=%s ORDER BY random()""",
        (set_id,),
    )
    items = [
        {'item_id': r[0], 'category': r[1], 'discipline': r[2],
         'distance': r[3], 'distance_unit': r[4], 'score': float(r[5])}
        for r in cur.fetchall()
    ]
    cur.close()
    conn.close()
    return render_template(
        'survey_rank.html',
        token=token, idx=idx, total=total,
        set_info=set_info, items=items,
        sid_name=resp['sid_name'],
    )


@app.route('/survey/<token>/<int:idx>/feedback', methods=['GET', 'POST'])
def survey_feedback(token, idx):
    """After ranking submit, show the user's order vs Adrian v3 order.
    POST advances to the next set or to the done page."""
    conn = get_connection()
    cur = conn.cursor()
    resp = _load_response(cur, token)
    total = len(resp['set_ids'])
    if idx < 1 or idx > total:
        cur.close(); conn.close()
        abort(404)
    set_id = resp['set_ids'][idx - 1]

    if request.method == 'POST':
        # Advance
        if idx == total:
            cur.execute(
                "UPDATE survey_responses SET completed_at=NOW() WHERE response_id=%s",
                (resp['response_id'],),
            )
            conn.commit(); cur.close(); conn.close()
            return redirect(url_for('survey_done', token=token))
        conn.commit(); cur.close(); conn.close()
        return redirect(url_for('survey_rank', token=token, idx=idx + 1))

    # GET: pull set + the user's rankings + Adrian's
    cur.execute(
        """SELECT competition_label, match_name, distance, distance_unit
           FROM survey_sets WHERE set_id=%s""", (set_id,),
    )
    sr = cur.fetchone()
    set_info = {
        'competition_label': sr[0], 'match_name': sr[1],
        'distance': sr[2], 'distance_unit': sr[3],
    }
    cur.execute(
        """SELECT i.item_id, i.category, i.discipline,
                  i.distance, i.distance_unit, i.score, i.centres,
                  i.adrian_v3, i.peter_score, i.v5_score,
                  r.rank, r.adrian_rank, r.peter_rank, r.v5_rank
           FROM survey_set_items i
           JOIN survey_rankings r
             ON r.item_id = i.item_id
            AND r.response_id = %s
            AND r.set_id = %s
           WHERE i.set_id = %s""",
        (resp['response_id'], set_id, set_id),
    )
    rows = cur.fetchall()
    items = [{
        'item_id': r[0], 'category': r[1], 'discipline': r[2],
        'distance': r[3], 'distance_unit': r[4],
        'score': float(r[5]), 'centres': r[6],
        'adrian': float(r[7]) if r[7] is not None else None,
        'peter':  float(r[8]) if r[8] is not None else None,
        'v5':     float(r[9]) if r[9] is not None else None,
        'user_rank': r[10], 'adrian_rank': r[11], 'peter_rank': r[12], 'v5_rank': r[13],
        'adrian_match': r[10] == r[11],
        'peter_match':  r[10] == r[12],
        'v5_match':     r[10] == r[13],
    } for r in rows]
    user_order   = sorted(items, key=lambda x: x['user_rank'])
    adrian_order = sorted(items, key=lambda x: x['adrian_rank'])
    peter_order  = sorted(items, key=lambda x: x['peter_rank'])
    v5_order     = sorted(items, key=lambda x: (x['v5_rank'] if x['v5_rank'] is not None else 99))
    adrian_matches = sum(1 for it in items if it['adrian_match'])
    peter_matches  = sum(1 for it in items if it['peter_match'])
    v5_matches     = sum(1 for it in items if it['v5_match'])
    cur.close(); conn.close()
    return render_template(
        'survey_feedback.html',
        token=token, idx=idx, total=total,
        set_info=set_info,
        user_order=user_order,
        adrian_order=adrian_order, peter_order=peter_order, v5_order=v5_order,
        n_items=len(items),
        adrian_matches=adrian_matches, peter_matches=peter_matches, v5_matches=v5_matches,
        sid_name=resp['sid_name'],
        is_last=(idx == total),
    )


@app.route('/survey/<token>/done')
def survey_done(token):
    conn = get_connection()
    cur = conn.cursor()
    resp = _load_response(cur, token)
    # Overall agreement stats vs both formulas
    cur.execute(
        """SELECT COUNT(*),
                  COUNT(*) FILTER (WHERE rank = adrian_rank),
                  AVG(ABS(rank - adrian_rank))::numeric(8,2),
                  COUNT(*) FILTER (WHERE rank = peter_rank),
                  AVG(ABS(rank - peter_rank))::numeric(8,2),
                  COUNT(*) FILTER (WHERE rank = v5_rank),
                  AVG(ABS(rank - v5_rank))::numeric(8,2),
                  COUNT(DISTINCT set_id)
           FROM survey_rankings
           WHERE response_id = %s AND adrian_rank IS NOT NULL""",
        (resp['response_id'],),
    )
    (n_items, adrian_matches, adrian_avg_diff,
     peter_matches, peter_avg_diff,
     v5_matches, v5_avg_diff, n_sets) = cur.fetchone()
    cur.close()
    conn.close()
    return render_template(
        'survey_done.html',
        sid_name=resp['sid_name'], email=resp['email'],
        total=len(resp['set_ids']),
        n_items=n_items or 0,
        adrian_matches=adrian_matches or 0,
        adrian_avg_diff=float(adrian_avg_diff) if adrian_avg_diff else 0.0,
        peter_matches=peter_matches or 0,
        peter_avg_diff=float(peter_avg_diff) if peter_avg_diff else 0.0,
        v5_matches=v5_matches or 0,
        v5_avg_diff=float(v5_avg_diff) if v5_avg_diff else 0.0,
        n_sets=n_sets or 0,
    )


if __name__ == '__main__':
    import os
    port = int(os.getenv('PORT', 5001))
    debug = os.getenv('FLASK_ENV') != 'production'
    app.run(host='0.0.0.0', port=port, debug=debug)

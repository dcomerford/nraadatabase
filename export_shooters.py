"""Export all shooters to a text file."""
from db import get_connection


def export_shooters():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute('''
        SELECT sh.sid, sh.first_name, sh.last_name, sh.pref_name, cl.club_name
        FROM shooters sh
        LEFT JOIN clubs cl ON sh.club_id = cl.club_id
        ORDER BY sh.last_name, sh.first_name;
    ''')

    shooters = cur.fetchall()

    with open('shooters.txt', 'w') as f:
        f.write(f"All Shooters ({len(shooters)} total)\n")
        f.write("=" * 80 + "\n\n")

        for sid, first_name, last_name, pref_name, club_name in shooters:
            name = f"{first_name} {last_name}"
            if pref_name:
                name += f" ({pref_name})"
            club = club_name if club_name else "No club"
            f.write(f"ID: {sid} | {name} | {club}\n")

    cur.close()
    conn.close()

    print(f"Exported {len(shooters)} shooters to shooters.txt")


if __name__ == "__main__":
    export_shooters()

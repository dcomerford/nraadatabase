import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# DATABASE_URL takes precedence (production / overrides). Default to local nraadb.
DATABASE_URL = os.getenv('DATABASE_URL') or 'postgresql:///nraadb'


def get_connection():
    """Create and return a database connection."""
    return psycopg2.connect(DATABASE_URL)


def test_connection():
    """Test the database connection."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"Connected successfully!\nPostgreSQL version: {version[0]}")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()

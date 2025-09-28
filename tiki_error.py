import psycopg2
from psycopg2 import sql

def create_tiki_error():
    conn = psycopg2.connect(
        dbname="tiki_crawler",
        user="mac",
        password="210412",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS Tiki_error (
        id BIGINT PRIMARY KEY,
        name TEXT,
        url_key TEXT,
        price NUMERIC,
        description TEXT,
        image_url TEXT,
        missing_fields TEXT,
        error_message TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Tiki_error table created")

if __name__ == "__main__":
    create_tiki_error()

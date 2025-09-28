import psycopg2
from config import load_config

def connect(db_config):
    try:
        with psycopg2.connect(**db_config) as conn:
            print('Connected')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    db_config = load_config()
    connect(db_config)
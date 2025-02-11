import psycopg2
from psycopg2 import sql
from psycopg2.pool import SimpleConnectionPool
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)



# PostgreSQL Connection Pool
CONNECTION_STRING = "postgresql://neondb_owner:npg_Mr7uaZH1pGBP@ep-morning-art-a9w8mj9y-pooler.gwc.azure.neon.tech/neondb?sslmode=require"
db_pool = SimpleConnectionPool(1, 10, dsn=CONNECTION_STRING)




def create_table_if_not_exists():
    """Creates the 'trades_fyers' table if it does not exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS trades_fyers (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        price DECIMAL(18,8) NOT NULL
    );
    """
    conn = None
    cursor = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logging.info("Table 'trades_fyers' ensured in the database.")
    except Exception as error:
        logging.error(f"Error creating table: {error}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)

create_table_if_not_exists()
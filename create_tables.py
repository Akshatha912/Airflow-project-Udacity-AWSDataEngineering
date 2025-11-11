import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all tables listed in drop_table_queries list.
    """
    for query in drop_table_queries:
        print(f"Dropping table with query: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all tables listed in create_table_queries list.
    """
    for query in create_table_queries:
        print(f"Creating table with query: {query}")
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads configuration from dwh.cfg
    - Connects to Redshift cluster
    - Drops existing tables
    - Creates new tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST = config.get("CLUSTER", "HOST")
    DB_NAME = config.get("CLUSTER", "DB_NAME")
    DB_USER = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DB_PORT = config.get("CLUSTER", "DB_PORT")

    conn = psycopg2.connect(
        f"host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

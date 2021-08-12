import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop all the tables listed if they exist in the redshift cluster
    :param cur: cursory object associated with the connection. 
    :param conn: psycopg2.connection object with access to Postgres database"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """"Create all of the staging, fact and dimension tables listed in create_table_queries
    to the redshift cluster"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    1-Read the configuration parameters 
    2-connect to the Redshift cluster
    3-Drop and create the staging, fact and dimension tables
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

import pandas as pd
import numpy as np
import configparser
import psycopg2
from psycopg2 import sql
from sql_queries import copy_table_queries, insert_table_queries
from time import time

def load_staging_tables(cur, conn):
    """Copy the JSON data (song and event) from S3 into staging tables in Redshift"""
    for query in copy_table_queries:
        print(query)
        t0 = time()
        cur.execute(query)
        conn.commit()
        loadTime = time()-t0
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))

def insert_tables(cur, conn):
    """Insert the data from the staging tables on Redshift into the fact and dimension tables"""
    for query in insert_table_queries:
        print(query)
        t0 = time()
        cur.execute(query)
        conn.commit()
        loadTime = time() - t0
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
        

def main():
    """
    1. Connect and create cursor to the redshift cluster
    2. Copy the JSON data (song and event) from S3 into staging tables in Redshift
    3. Insert the data from the staging tables on Redshift into the fact and dimension tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load staging tables
    load_staging_tables(cur, conn)
    
    # load analytics tables
    insert_tables(cur, conn)
    
    conn.close()

if __name__ == "__main__":
    main()    
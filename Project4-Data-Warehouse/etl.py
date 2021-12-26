import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Calls procedure to copy tables from S3 to Redshift staging area
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - Calls procedure to run insert queries which will populate analytical tables in Redshift with the data in the Redshift staging area
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Main function that will allow running of the S3 to Redshift ETL process
    - Establishes connection to Redshift Cluster
    - Gets cursor for PostgreSQL session
    - Calls functions load_staging_tables and insert_tables which will perform the ETL processes
    - Closes connection to Redshift Cluster
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
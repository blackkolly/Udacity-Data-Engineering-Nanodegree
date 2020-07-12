import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """drop database tables from drop_table_queries, 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ create database tables from create_table_queries, 
    and insert values into the table
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('existing tables dropped')
    drop_tables(cur, conn)
    
    print('Creating tables')
    create_tables(cur, conn)

   

    conn.close()


if __name__ == "__main__":
    main()
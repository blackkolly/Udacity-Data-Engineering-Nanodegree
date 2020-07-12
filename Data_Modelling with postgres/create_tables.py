import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
       - Creates and connects to the sparkifydb
       - Returns the connection and cursor to sparkifydb
    """
    # Establishing the connection to database
    conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres", password="Koladudu22.", port="5433")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(user="postgres",
                                  password="postgres.",
                                  host="127.0.0.1",
                                  port="5433",
                                  database="sparkifydb")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
        Drops each table using the queries in `drop_table_queries` list.
        """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
      Creates each table using the queries in `create_table_queries` list.
      """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
       The Main function establishes a connection to the database, drops all tables
       and create the tables
       """
    cur, conn = create_database()
    create_tables(cur, conn)
    drop_tables(cur, conn)

# close the database connection
    conn.close()


if __name__ == "__main__":
    main()

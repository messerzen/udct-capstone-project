import configparser
import psycopg2
from sql_queries import create_tables_queries, load_data_into_table


def create_fact_dimension_tables(cur, conn):
    """Creates fact and dimensions tables in amazon redshift staging tables.

    Args:
        cur (object): cursor for interact with aws redshift cluster.
        conn (object): connection with amazon redshift cluster.
    """
    for query in create_tables_queries:
        cur.execute(query)
        conn.commit()


def load_data_into_tables(cur, conn):
    """Inputs data from s3 bucket to the fact and dimensions table in amazon redshift cluster.

    Args:
        cur (object): cursor for interact with aws redshift cluster.
        conn (object): connection with amazon redshift cluster.
    """

    for query in load_data_into_table:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Creates a config parser object.
    - Parses the aws_paths.cgf file (amazon aws connection informations).
    - Creates an amazon redshift connnection object using psycopg2 library.
    - Creates a cursor object to interact with amazon redshift cluster.
    - Creates fact and dimensions tables in amazon redshift.
    - Inserts data from s3 files to dimensions and fact tables.
    """
    config = configparser.ConfigParser()
    config.read('aws_paths.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    create_fact_dimension_tables(cur, conn)
    load_data_into_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
import configparser
import psycopg2

def get_number_of_records_from_table(table_name):

    config = configparser.ConfigParser()
    config.read('../etl/aws_paths.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM {}'.format(table_name))
    total_records = cur.fetchone()[0]
    conn.close()

    return total_records

def get_number_of_distinct_records_from_table_column(table_name, column_name):

    config = configparser.ConfigParser()
    config.read('../etl/aws_paths.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    cur.execute('SELECT COUNT(DISTINCT({})) FROM {}'.format(column_name, table_name))
    total_records = cur.fetchone()[0]
    conn.close()

    return total_records
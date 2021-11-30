from get_redshift_data import get_number_of_distinct_records_from_table_column,\
                              get_number_of_records_from_table
from get_s3_data import get_number_of_records_from_s3_files
import configparser

config = configparser.ConfigParser()
config.read('/home/zen/Desenvolvimento/Environments/capstone-dataengineer/etl/aws_paths.cfg')
STABLISHMENTS_RAW = config['S3']['STABLISHMENTS_RAW']

FACT_TABLE_CNPJ_PATH = config['S3']['FACT_TABLE_CNPJ']
CNAES_DATA = config['S3']['CNAES_DATA']
CITIES_DATA = config['S3']['CITIES_DATA']


def test_compare_number_of_registers_cnpjreceitafederal_table():
    total_record_of_db_table = get_number_of_records_from_table('cnpj_receita_federal')
    total_records_of_csv_raw_file = get_number_of_records_from_s3_files('{}'.format(FACT_TABLE_CNPJ_PATH))
    assert total_record_of_db_table == total_records_of_csv_raw_file

def test_compare_number_of_registers_cnaes_table():
    total_record_of_db_table = get_number_of_records_from_table('cnaes')
    total_records_of_csv_raw_file = get_number_of_records_from_s3_files('{}'.format(CNAES_DATA))
    assert total_record_of_db_table == total_records_of_csv_raw_file

def test_compare_number_of_registers_cities_table():
    total_record_of_db_table = get_number_of_records_from_table('cities')
    total_records_of_csv_raw_file = get_number_of_records_from_s3_files('{}'.format(CITIES_DATA))
    assert total_record_of_db_table == total_records_of_csv_raw_file

def test_compare_number_of_discint_key_agains_all_registers_table():
    total_record_of_db_table = get_number_of_records_from_table('cnpj_receita_federal')
    total_records_of_csv_raw_file = get_number_of_distinct_records_from_table_column('cnpj_receita_federal', 'cnpj')
    assert total_record_of_db_table == total_records_of_csv_raw_file
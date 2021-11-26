import configparser

config = configparser.ConfigParser()
config.read('aws_paths.cfg')
IAM=config['IAM_ROLE']['ARN']
FACT_TABLE_CNPJ=config['S3']['FACT_TABLE_CNPJ']
CITIES_DATA = config['S3']['CITIES_DATA']
CNAES_DATA=config['S3']['CNAES_DATA']

# CREATE STATEMENTS
CREATE_CITIES_TABLE = '''
    CREATE TABLE cities (
        codigo_ibge INT,
        nome VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL,
        capital BOOLEAN, 
        codigo_uf INT,
        siafi_id INT,
        ddd INT,
        fuso_horario VARCHAR
    )
'''

CREATE_CNAES_TABLE = '''
    CREATE TABLE cnaes (
        cod_cnae INT,
        cnae_description VARCHAR
    )
'''

CREATE_CNPJS_RECEITA_FEDERAL_TABLE = '''
    CREATE TABLE cnpj_receita_federal (
            cnpj_basico VARCHAR,
            cnpj VARCHAR,
            matriz_filial VARCHAR,
            razao_social VARCHAR,
            dt_inicioatividade DATE,
            cod_cnae INT,
            cep INT,
            cod_municipio INT, 
            porte VARCHAR,
            phone_contact VARCHAR,
            contact_email VARCHAR 
        )
'''

# COPY statements
COPY_FACT_TABLE_DATA = '''
    COPY cnpj_receita_federal
    FROM '{}'
    IAM_ROLE '{}'
    CSV DELIMITER ';' 
    IGNOREHEADER 1;
'''.format(FACT_TABLE_CNPJ, IAM)

COPY_DIMENSION_CITIES_DATA = '''
    COPY cities
    FROM '{}'
    IAM_ROLE '{}'
    CSV DELIMITER ';' 
    IGNOREHEADER 1;
'''.format(CITIES_DATA, IAM)

COPY_DIMENSION_CNAE_DATA = '''
    COPY cnaes
    FROM '{}'
    IAM_ROLE '{}'
    CSV DELIMITER ';' 
    IGNOREHEADER 1;
'''.format(CNAES_DATA, IAM)

create_tables_queries = [CREATE_CITIES_TABLE, CREATE_CNAES_TABLE, CREATE_CNPJS_RECEITA_FEDERAL_TABLE]
load_data_into_table = [COPY_FACT_TABLE_DATA, COPY_DIMENSION_CITIES_DATA, COPY_DIMENSION_CNAE_DATA]
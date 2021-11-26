from pyspark.sql import SparkSession
from spark_session_creator import create_spark_session
import configparser

def create_dimension_table():

    spark = create_spark_session()

    config = configparser.ConfigParser()
    config.read('aws_paths.cfg')

    companies_clean_data_path = config['S3']['COMPANIES_CLEANED']
    stablishments_clean_data_path = config['S3']['STABLISHMENTS_CLEANED']
    fact_table_path = config['S3']['FACT_TABLE_CNPJ']

    companies_df = spark.read.parquet(companies_clean_data_path)
    stablishments_df = spark.read.parquet(stablishments_clean_data_path)

    companies_df.createOrReplaceTempView('companies_view')
    stablishments_df.createOrReplaceTempView('stablishments_view')

    dimension_table = spark.sql('''
       SELECT a.cnpj_basico,
           a.cnpj_basico || a.cnpj_ordem || a.cnpj_dv AS cnpj,
           CASE WHEN a.cod_matfil = 1 THEN 'MATRIZ' 
                WHEN a.cod_matfil = 0 THEN 'FILIAL'
           END AS matriz_filial,
           b.razao_social,
           a.dt_inicioatividade,
           a.cod_cnae,
           a.add_cep AS cep,
           a.add_cod_mun AS cod_municipio,
           CASE WHEN b.cod_porte = 2 THEN 'MICRO-EMPRESA'
                WHEN b.cod_porte = 3 THEN 'PEQUENO PORTE'
                WHEN b.cod_porte = 5 THEN 'DEMAIS'
                ELSE 'NAO INFORMADO' 
            END AS porte,
            a.contact_ddd1 || a.contact_phone1 AS phone_contact,
            a.contact_email    
    FROM stablishments_view a
    JOIN companies_view b
    ON a.cnpj_basico = b.cnpj_basico
        ''')

    dimension_table.write.mode('overwrite').parquet(fact_table_path)
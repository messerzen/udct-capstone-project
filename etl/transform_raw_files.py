from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import regexp_replace
import configparser
from spark_session_creator import create_spark_session

def extract_transform_raw_files():
    """Parses the stablisments and companies raw files from s3. 
       Transform stablishments and companies files.
       Create the fact table schema using SchemaOnRead.
       Write the dataframe with fact table schema in S3.
    """

    config = configparser.ConfigParser()
    config.read('/home/zen/Desenvolvimento/Environments/capstone-dataengineer/etl/aws_paths.cfg')
    spark = create_spark_session()


    STABLISHMENTS_RAW = config['S3']['STABLISHMENTS_RAW']
    print(STABLISHMENTS_RAW)

    COMPANIES_RAW = config['S3']['COMPANIES_RAW']
    FACT_TABLE_CNPJ_PATH = config['S3']['FACT_TABLE_CNPJ']
  
    STABLISHMENT_STRUCTURE = StructType([
            StructField('cnpj_basico', StringType(), nullable=False),
            StructField('cnpj_ordem', StringType(), nullable=False),
            StructField('cnpj_dv', StringType(), nullable=False),
            StructField('cod_matfil', IntegerType(), nullable=False),
            StructField('nome_fantasia', StringType(), nullable=True),
            StructField('cod_sitcad', IntegerType(), nullable=True),
            StructField('dt_sitcad', DateType(), nullable=True),
            StructField('codmot_sitcad', StringType(), nullable=True),
            StructField('cidade_exterior', StringType(), nullable=True),
            StructField('cod_pais', StringType(), nullable=True),
            StructField('dt_inicioatividade', DateType(), nullable=True),
            StructField('cod_cnae', IntegerType(), nullable=False),
            StructField('cod_cnae_secundario', StringType(), nullable=True),
            StructField('add_tipo_logradouro', StringType(), nullable=True),
            StructField('add_logradouro', StringType(), nullable=True),
            StructField('add_numero', StringType(), nullable=True),
            StructField('add_complemento', StringType(), nullable=True),
            StructField('add_bairro', StringType(), nullable=True),
            StructField('add_cep', IntegerType(), nullable=True),
            StructField('add_uf', StringType(), nullable=True),
            StructField('add_cod_mun', IntegerType(), nullable=True),
            StructField('contact_ddd1', StringType(), nullable=True),
            StructField('contact_phone1', StringType(), nullable=True),
            StructField('contact_ddd2', StringType(), nullable=True),
            StructField('contact_phone2', StringType(), nullable=True),
            StructField('contact_ddd_fax', StringType(), nullable=True),
            StructField('contact_fax', StringType(), nullable=True),
            StructField('contact_email', StringType(), nullable=True),
            StructField('cod_sitespecial', StringType(), nullable=True),
            StructField('dt_sitespecial', DateType(), nullable=True)]
        )


    COMPANY_STRUCTURE =  StructType ([
                StructField('cnpj_basico', StringType(), nullable=False),
                StructField('razao_social', StringType(), nullable=False),
                StructField('cod_natjuridica', IntegerType(), nullable=True),
                StructField('cod_qualiresponsavel', IntegerType(), nullable=True),
                StructField('capital_social', StringType(), nullable=True),
                StructField('cod_porte', IntegerType(), nullable=True),
                StructField('ente_fed', StringType(), nullable=True)]
    )

    companies_df = spark.read.option("delimiter",';')\
                    .option("emptyValue", '""')\
                    .option("dateFormat",'yyyyMMdd')\
                    .option("encoding",'iso-8859-1')\
                    .option('header', 'false')\
                    .schema(COMPANY_STRUCTURE)\
                    .csv(COMPANIES_RAW)

    stablishments_df = spark.read.option("delimiter",';')\
                    .option("emptyValue", '""')\
                    .option("dateFormat",'yyyyMMdd')\
                    .option("encoding",'iso-8859-1')\
                    .option('header', 'false')\
                    .schema(STABLISHMENT_STRUCTURE)\
                    .csv(STABLISHMENTS_RAW)

    for column in companies_df.columns:
        companies_df = companies_df.withColumn(column, regexp_replace(column, '"',"")) 
    capital_social_to_float_df = companies_df.withColumn('capital_social', regexp_replace('capital_social', ',', '').cast(FloatType()))

    for column in stablishments_df.columns:
        stablishments_df = stablishments_df.withColumn(column, regexp_replace(column, '"',"")) 
    active_stablishments_df = stablishments_df.filter(stablishments_df.cod_sitcad == 2)

    capital_social_to_float_df.createOrReplaceTempView('companies_view')
    active_stablishments_df.createOrReplaceTempView('stablishments_view')

    fact_table = spark.sql('''
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

    fact_table.write.option('delimiter',';').option('header', 'true').csv(FACT_TABLE_CNPJ_PATH)

def main():
    '''Executes extract_transform_raw_files
    '''
    extract_transform_raw_files()
   
if __name__ == '__main__':
    main()
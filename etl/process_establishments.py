from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import regexp_replace
#import logging
from spark_session_creator import create_spark_session

def process_stablishments(from_path_folder, to_path_folder):
  
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

    spark = create_spark_session()

    stablishments_df = spark.read.option("delimiter",';')\
                    .option("emptyValue", '""')\
                    .option("dateFormat",'yyyyMMdd')\
                    .option("encoding",'iso-8859-1')\
                    .option('header', 'false')\
                    .schema(STABLISHMENT_STRUCTURE)\
                    .csv(from_path_folder)

    for column in stablishments_df.columns:
        stablishments_df = stablishments_df.withColumn(column, regexp_replace(column, '"',"")) 
    active_stablishments_df = stablishments_df.filter(stablishments_df.cod_sitcad == 2)
    active_stablishments_df.write\
                            .mode('overwrite')\
                            .partitionBy('cod_cnae')\
                            .parquet(to_path_folder)

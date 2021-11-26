from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import regexp_replace, udf, col
#import logging
from spark_session_creator import create_spark_session

def process_companies(from_path_folder, to_path_folder):
    
    COMPANY_STRUCTURE =  StructType ([
                StructField('cnpj_basico', StringType(), nullable=False),
                StructField('razao_social', StringType(), nullable=False),
                StructField('cod_natjuridica', IntegerType(), nullable=True),
                StructField('cod_qualiresponsavel', IntegerType(), nullable=True),
                StructField('capital_social', StringType(), nullable=True),
                StructField('cod_porte', IntegerType(), nullable=True),
                StructField('ente_fed', StringType(), nullable=True)]
    )

    spark = create_spark_session()
    spark_context = spark.sparkContext


    companies_df = spark.read.option("delimiter",';')\
                    .option("emptyValue", '""')\
                    .option("dateFormat",'yyyyMMdd')\
                    .option("encoding",'iso-8859-1')\
                    .option('header', 'false')\
                    .schema(COMPANY_STRUCTURE)\
                    .csv(from_path_folder)
    for column in companies_df.columns:
        companies_df = companies_df.withColumn(column, regexp_replace(column, '"',"")) 
    capital_social_to_float_df = companies_df.withColumn('capital_social', regexp_replace('capital_social', ',', '').cast(FloatType()))

    #logging.info(f'Writing parquet files')
    capital_social_to_float_df.write\
                .mode('overwrite')\
                .parquet(to_path_folder)
    #logging.info(f'Companies processed files saved in {to_path_folder}')    


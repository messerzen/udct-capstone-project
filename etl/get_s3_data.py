
from spark_session_creator import create_spark_session

def get_number_of_records_from_s3_files(s3_folder_path):

    spark_session = create_spark_session()
    fact_table_df = spark_session.read.option("delimiter",';')\
                            .option("encoding",'iso-8859-1')\
                            .option('header', 'true')\
                            .csv(s3_folder_path)
    
    number_of_records = fact_table_df.count()
    return number_of_records
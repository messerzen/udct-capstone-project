from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession.builder.getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=CORRECTED")
    return spark

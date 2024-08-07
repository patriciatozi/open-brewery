from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

bronze_schema = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("brewery_type", StringType(), True),
                StructField("address_1", StringType(), True),
                StructField("address_2", StringType(), True),
                StructField("address_3", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state_province", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("country", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("website_url", StringType(), True),
                StructField("state", StringType(), True),
                StructField("street", StringType(), True)
                ])

def start_spark_session(spark_path, hadoop_path):

    spark = SparkSession.builder \
            .appName("BreweriesData") \
            .config("spark.driver.extraClassPath", spark_path) \
            .config("spark.jars.packages", hadoop_path) \
            .getOrCreate()

    return spark
# Start Spark Session
from core.utils import start_spark_session, bronze_schema
from core.api import get_max_page, get_open_brewery_api
from core.constants import items_per_page
from core.storage import load_data_to_azure_sql_db_table
from pyspark.sql import Row
import toml
import os

os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'  
os.environ['SPARK_HOME'] = '/opt/spark'

if __name__ == '__main__':

    with open('config.toml', 'r') as f:
        config = toml.load(f)

    spark = start_spark_session(config['spark-config']['spark_path'], \
                                config['spark-config']['hadoop_path'])
    
    max_pages = get_max_page(config['open-brewery-db']['main_url'], items_per_page)

    all_data_spark = None

    for page in range(1,max_pages+1):

        breweries_data = get_open_brewery_api(config['open-brewery-db']['main_url'], \
                                              page, items_per_page)
        
        rdd = spark.sparkContext.parallelize([Row(**brewery) for brewery in breweries_data])

        bronze_dataframe = spark.createDataFrame(rdd, bronze_schema)

        if all_data_spark is None:
            all_data_spark = bronze_dataframe
        else:
            all_data_spark = all_data_spark.union(bronze_dataframe)

        
    azure_db = {
        'server': config['azure-sql-databases']['server'],
        'database': config['azure-sql-databases']['database'],
        'username': config['azure-sql-databases']['username'],
        'password': config['azure-sql-databases']['password']
    }

    
    sucessful_loading_db = load_data_to_azure_sql_db_table(all_data_spark, azure_db, "bronze_api_brewery_dev")

    # if sucessful_loading_db:

    #     azure_account_storage = {
    #         'storage_account_name': config['azure-account-storage']['storage_account_name'],
    #         'storage_account_key': config['azure-account-storage']['storage_account_key'],
    #         'container_name': config['azure-account-storage']['container_name']
    #     }
        

    






def load_data_to_azure_sql_db_table(dataframe, credentials, table_name):

    print(credentials['server'])

    jdbc_url = f"jdbc:sqlserver://{credentials['server']}:1433;databaseName={credentials['database']};user={credentials['username']};password={credentials['password']};encrypt=true;trustServerCertificate=false;"

    connection_properties = {
        "user": credentials['username'],
        "password": credentials['password'],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    dataframe.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

    return True

def load_backup_parque_file_to_azure_account_storage(spark, dataframe, credentials, layer):

    output_path = f"wasbs://{credentials['container_name']}@{credentials['storage_account_name']}.blob.core.windows.net/{layer}"

    spark.conf.set(f"fs.azure.account.key.{credentials['storage_account_name']}.blob.core.windows.net", credentials['storage_account_key'])

    dataframe.write.partitionBy('country', 'city').parquet(output_path, mode='overwrite')

    print(f"DataFrame written to {output_path}")
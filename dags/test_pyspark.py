from airflow import DAG
from core.utils import start_spark_session
import toml

with open('/opt/airflow/config.toml', 'r') as f:
    config = toml.load(f)

print(config['open-brewery-db']['main_url'])

spark = start_spark_session()
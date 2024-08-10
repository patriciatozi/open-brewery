from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from core.api import get_max_page
import toml

def load_config():
    with open('/opt/airflow/config.toml', 'r') as f:
        config = toml.load(f)
    return config

def print_main_url():
    config = load_config()
    main_url = config['open-brewery-db']['main_url']
    print(main_url)

def calculate_max_pages():
    config = load_config()
    max_pages = get_max_page(config['open-brewery-db']['main_url'], 100)
    print(max_pages)
    return max_pages

# Definindo a DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='open_brewery_db_dag',
    default_args=default_args,
    description='DAG para processar dados da API Open Brewery DB',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Tarefa para imprimir a URL principal
    task_print_main_url = PythonOperator(
        task_id='print_main_url',
        python_callable=print_main_url,
    )

    # Tarefa para calcular e imprimir o número máximo de páginas
    task_calculate_max_pages = PythonOperator(
        task_id='calculate_max_pages',
        python_callable=calculate_max_pages,
    )

    # Definir a ordem das tarefas
    task_print_main_url >> task_calculate_max_pages
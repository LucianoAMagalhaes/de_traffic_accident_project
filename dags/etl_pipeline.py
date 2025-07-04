from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess

default_args = {
    'owner': 'luciano',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='etl_pipeline_acidentes',
    default_args=default_args,
    description='Pipeline ETL de acidentes de trânsito com Airflow',
    schedule_interval=None,  # você pode mudar para '0 8 * * *' para rodar todo dia às 8h
    catchup=False
)

# Caminhos relativos ao DAG
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.join(BASE_DIR, 'scripts')

def run_transform_script():
    subprocess.run(['python', os.path.join(SCRIPT_DIR, 'transform.py')], check=True)

def run_load_script():
    subprocess.run(['python', os.path.join(SCRIPT_DIR, 'load.py')], check=True)

transform_task = PythonOperator(
    task_id='transformar_dados',
    python_callable=run_transform_script,
    dag=dag
)

load_task = PythonOperator(
    task_id='carregar_dados',
    python_callable=run_load_script,
    dag=dag
)

transform_task >> load_task

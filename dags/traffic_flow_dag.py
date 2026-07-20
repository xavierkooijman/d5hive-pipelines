from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import yaml
from utils.logger import get_logger
from ingestion.pipelines.traffic_flow import TrafficFlowPipeline

with open("pipelines_config/traffic_flow.yaml", "r") as f:
    config = yaml.safe_load(f)


def run_traffic_flow():
    get_logger()
    TrafficFlowPipeline(config).run()


with DAG(
    dag_id='traffic_flow_ingestion',
    schedule='*/15 * * * *',
    start_date=datetime(2026, 5, 18),
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    PythonOperator(
        task_id='run_traffic_flow',
        python_callable=run_traffic_flow,
    )

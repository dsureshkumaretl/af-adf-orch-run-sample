from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator


with DAG(
    dag_id="example_adf_run_pipeline",
    start_date=datetime(2024, 1, 14),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "airflow_datafactory", #This is a connection created on Airflow UI
    },
    default_view="graph",
) as dag:

    run_adf_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_adf_pipeline",
        pipeline_name="testsnowflake",
    )

    run_adf_pipeline

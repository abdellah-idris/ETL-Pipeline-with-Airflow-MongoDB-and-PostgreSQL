from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from etl import run_etl

# Default arguments for the DAG
default_args = {
    'owner': 'idris',
    'start_date': days_ago(0),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG
with DAG(
        dag_id='ETL_TWITTER',
        default_args=default_args,
        description='Extract, Transform and Load Twitter data',
        schedule=timedelta(days=1)
) as dag:
    # Define ETL task
    etl = PythonOperator(
        task_id='extract',
        python_callable=run_etl,  # this should call your script's main function
        op_kwargs={},  # pass any arguments to your script here
        dag=dag
    )

    hello = BashOperator(task_id="hello", bash_command="echo ETL ended")
    # Set up the DAG's structure
    # TODO : a clear to data base task
    hello

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'sreeji',
    'start_date': datetime(2018, 8, 26),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('SparkStart', description='Starter',
          schedule_interval='* * * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_job',
    conn_id='spark_default',
    java_class='com.imply.assessment.one.DistinctPath',
    application='/Users/sreeji/Documents/Sreeji/work/ImplyIo/target/imply.io-1.0-SNAPSHOT-jar-with-dependencies.jar',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='2',
    name='spark-airflow-phoenix',
    verbose=True,
    driver_memory='1g',
    xcom_push='true',
    dag=dag,
)

def print_hello():
    return 'Finally it worked!!!!' + str(datetime.now().strftime("%m%d%Y-%H%M"))

def print_check():
    return 'Finally it worked!!!!' + str(datetime.now().strftime("%m%d%Y-%H%M"))

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

spark_success = PythonOperator(task_id='spark_success_task',python_callable=print_hello, dag=dag)

dummy_operator >> spark_submit_task >> spark_success

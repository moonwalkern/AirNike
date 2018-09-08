from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator

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
    java_class='com.verizon.gsoc.datasources.phoenix.Phoenix',
    jars='/Users/gopalsr/Documents/Sreeji/code/AirNike/first_folder/spark-druid-etl-1.0-0-jar-with-dependencies.jar',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='2',
    name='spark-airflow-phoenix',
    verbose=True,
    driver_memory='1g',
    conf={
        'spark.DB_URL': 'jdbc:db2://dashdb-dal13.services.dal.bluemix.net:50001/BLUDB:sslConnection=true;',
        'spark.DB_USER': 'value'
    },
    dag=dag,
)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

dummy_operator >> spark_submit_task

import os
import urllib
from datetime import datetime, timedelta
from subprocess import call

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def split_call(str):
    call(str.split(' '))

def print_hello():
    return 'Finally it worked!!!!' + str(datetime.now().strftime("%m%d%Y-%H%M"))

def check_file(paths):
    try:
        for path in paths.split(","):
            for root, subdirs, files in os.walk(path):
                for file in files:
                    filename = urllib.quote(str(root) + '/' + file)
                    print filename
                    print 'hdfs dfs -put {0} /airflow/cp/'.format(urllib.quote(filename))
                    split_call(
                        'hdfs dfs -put {0} /airflow/cp/'.format(filename)
                    )


    except Exception as e:
        print "error " + str(e) + " rrr"
        pass

check_file("/Users/sreeji/airflow")

default_args = {
    'owner': 'sreeji',
    'start_date': datetime(2018, 8, 26),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}


def spark_submit(exec_path, params):
    params = params.copy()
    # params['log.schema'] = ','.join(params['log.schema'])

    s = ' '.join(['{0}={1}'.format(k, v) for (k, v) in params.iteritems()])

    print s
    # split_call(
    #     'spark-submit --master yarn --deploy-mode cluster --driver-memory 2g --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true --class com.verizon.gsoc.ProxyBlueCoat {0} {1}'.format(
    #         exec_path, ' '.join(['{0}={1}'.format(k, v) for (k, v) in params.iteritems()]))
    # )

#dag = DAG('hdfs_airflow', default_args=default_args, schedule_interval=timedelta(seconds=5))

dag = DAG('hello_world', description='Simple tutorial DAG',
          schedule_interval='* * * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

copy_hdfs = BashOperator(
    task_id='copy_hdfs',
    bash_command='hdfs dfs -mkdir /airflow/' + str(datetime.now().strftime("%m%d%Y-%H%M")),
    retries=3,
    dag=dag
)

spark = SparkSubmitOperator

file_check = PythonOperator(
    task_id='file_check1',
    python_callable=check_file, dag=dag
)

dummy_operator >> hello_operator >> copy_hdfs



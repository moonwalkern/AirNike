import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.sensors.hdfs_sensor import HdfsSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    'owner': 'gsoc',
    'start_date': datetime(2018, 8, 26),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('PhoenixFlow', description='Proxy Flow',
          schedule_interval='* * * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


# phoenix_batch_task = PhonixBatchOperator(phoenix_batch_operator_param='This is phoenix operator',
#                                          task_id='phoenix_batch_task', dag=dag)

# HADOOP_CLASSPATH=/home/hbpdev/hbase-1.3.1/lib/hbase-protocol-1.3.1.jar:/home/hbpdev/hbase-1.3.1/conf hadoop jar /home/hbpdev/phoenix-4.7.0-HBase-1.1-bin/phoenix-4.7.0-HBase-1.1-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool --table PROXY_DEV --input /user/enriched/02/date=01-02-2018/* -d $'\t'


def _build_phoenix_submit_command(table, path):
    """
    Construct the phoenix-submit command to execute.
    :param application: command to append to the phoenix-submit command
    :type application: str
    :return: full command to be executed
    """
    # connection_cmd = ['HADOOP_CLASSPATH=/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/etc/hbase/conf']

    # The url ot the spark master
    # connection_cmd += ["--master", self._connection['master']]
    connection_cmd = ['hadoop']
    connection_cmd += ['jar']
    connection_cmd += ['/usr/hdp/current/phoenix-client/phoenix-4.7.0.2.6.4.0-91-client.jar']
    connection_cmd += ['org.apache.phoenix.mapreduce.CsvBulkLoadTool']
    connection_cmd += ["--table", table]
    connection_cmd += ["--input", path]
    connection_cmd += ["-d\t"]
    log.info("Phoenix-Submit cmd: %s", connection_cmd)

    return connection_cmd


def _process_phoenix_submit_log(itr):
    """
    Processes the log files and extracts useful information out of it.

    If the deploy-mode is 'client', log the output of the submit command as those
    are the output logs of the Spark worker directly.

    Remark: If the driver needs to be tracked for its status, the log-level of the
    spark deploy needs to be at least INFO (log4j.logger.org.apache.spark.deploy=INFO)

    :param itr: An iterator which iterates over the input of the subprocess
    """
    # Consume the iterator
    for line in itr:
        line = line.strip()
        log.info(line)


def start_python_batch(**kwargs):
    print(" 'arguments' was passed in via task params = {}".format(kwargs.get('table')))
    table = kwargs.get('table')
    path = kwargs.get('path')
    # command = 'HADOOP_CLASSPATH=/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/etc/hbase/conf hadoop jar /usr/hdp/current/phoenix-client/phoenix-4.7.0.2.6.4.0-91-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool --table PROXY_DEV --input /user/enriched/02/date=01-02-2018/*'
    command = _build_phoenix_submit_command(table, path)
    log.info('Phoenix Batch Command : ' + str(command))
    os.putenv("HADOOP_CLASSPATH", "/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/etc/hbase/conf")
    _submit_sp = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1,
        universal_newlines=True)
    _process_phoenix_submit_log(iter(_submit_sp.stdout.readline, ''))
    returncode = _submit_sp.wait()
    log.info('Starting Python Batch process' + str(datetime.now().strftime("%m%d%Y-%H%M")) + ' return code ' + str(
        returncode))

    return returncode


phoenix_batch_task = PythonOperator(task_id='phoenix_batch_task', python_callable=start_python_batch,
                                    op_kwargs={'table': 'proxy_free',
                                               'path': '/user/spock/proxy/enriched/frhnjslg01.gsoc.verizon.com/2018/01/01/date=*/part*'},
                                    dag=dag)


def print_hello():
    return 'Finally it worked!!!!' + str(datetime.now().strftime("%m%d%Y-%H%M"))


def print_check():
    return 'Finally it worked!!!!' + str(datetime.now().strftime("%m%d%Y-%H%M"))


dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

spark_success = PythonOperator(task_id='spark_proxy_task', python_callable=print_hello,
                               dag=dag)
hdfs_sensor = HdfsSensor(task_id="hdfs",filepath="", poke_interval=30, dag=dag)

# bash_sensor = BashSensor(task_id="bash", bash_command="ls -lrt", poke_interval=30, dag=dag)
dummy_operator >> phoenix_batch_task >> spark_success

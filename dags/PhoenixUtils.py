import logging
import os
import subprocess
from datetime import datetime

from HdfsUtils import pullXcom

log = logging.getLogger(__name__)



def processPhoenixSubmitLog(itr):
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

def buildPhoenixSubmitCommand(table, path):
    # connection_cmd = ['HADOOP_CLASSPATH=/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/etc/hbase/conf']

    # The url ot the spark master
    # connection_cmd += ["--master", self._connection['master']]
    connection_cmd = ['hadoop']
    connection_cmd += ['jar']
    connection_cmd += ['/usr/hdp/current/phoenix-client/phoenix-client.jar']
    connection_cmd += ['org.apache.phoenix.mapreduce.CsvBulkLoadTool']
    connection_cmd += ["--table", table]
    connection_cmd += ["--input", path + "/*"]
    connection_cmd += ["-a \';\'"]
    connection_cmd += ["-d\t"]

    log.info("Phoenix-Submit cmd: %s", connection_cmd)

    return connection_cmd

def startPythonBatch(task_id, **kwargs):
    task = 'init_task_{}'.format(task_id)
    print(kwargs)
    ti = kwargs['ti']
    xcom_values = pullXcom(ti, task, "log_enrich_path_phoenix,table")  #pull data from xcom object
    table = xcom_values['table']  #pull data from xcom object
    path = xcom_values['log_enrich_path_phoenix']
    command = buildPhoenixSubmitCommand(table, path)
    log.info('Phoenix Batch Command : ' + str(command))
    os.putenv("HADOOP_CLASSPATH", "/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/etc/hbase/conf")
    submitSP = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1,
        universal_newlines=True)
    processPhoenixSubmitLog(iter(submitSP.stdout.readline, ''))
    returncode = submitSP.wait()
    log.info('Starting Python Batch process ' + str(datetime.now().strftime("%m%d%Y-%H%M")) + ' return code ' + str(returncode))

    return returncode

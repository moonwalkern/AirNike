import os, subprocess
import shutil
import stat
import logging
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.contrib.hooks.fs_hook import FSHook
# from MetricUtils import metics_fetch_files_fs, metics_files_count_hdfs, metics_files_lines_hdfs, pushMetices, metics_fetch_files_ssh_fs
from datetime import datetime, timedelta, date
from select import select

from airflow import configuration
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException

from base64 import b64encode

log = logging.getLogger(__name__)


import softBugConfig



xcom_keys = 'date,fs_path,hdfs_path,hdfs_path_month,log_enrich_path,log_enrich_path_phoenix,log_iplookup_path,glo_path,fs_pattern,params,fs_file,index_template,source,location'

def run_cmd(args_list, local):
    """
    run linux commands
    """
    # print('Running system command: {0}'.format(' '.join(args_list)))
    if local:
        proc = subprocess.Popen(args_list, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def copyFileToHDFS(local_path, hdfs_path):
    try:
        #check if the folder exist or not. if yes remove the files from the folder, as we need fresh files for spark job
        print('check if the folder exist')
        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-test','-d',hdfs_path],False)
        if ret == 0:
            print('folder not empty, deleting the files.')
            (ret, out, err) = run_cmd(['hdfs', 'dfs', '-rm','-r',hdfs_path +'/*'],False)
            if ret == 0:
                print('files removal success')
            else:
                print('files removal unsuccess')
        print('creating folder in HDFS location --> {}'.format(hdfs_path))
        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-mkdir', '-p', hdfs_path], False)
        files_n_folder = os.listdir(local_path)
        print('-------------------------------')
        print('list of files in LOCAL location')
        for file in files_n_folder:
            print(file)
        print('-------------------------------')
        print('copying file from LOCAL location {} to ---> HDFS location {}'.format(local_path, hdfs_path))
        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-put', local_path, hdfs_path + "/"], False)
        if err:
            print('file copy failed {}'.format(err))
        if ret:
            print('file copy success {}'.format(ret))
            (ret, out, err) = run_cmd(['hdfs', 'dfs', '-ls', '-C', hdfs_path + '/'], False)
            print('files in the HDFS path {} -->'.format(hdfs_path))
            for file in out.split("\n"):
                print(file)

        return ret
    except Exception as e:
        print("error " + str(e) + " folder not good!!!")
        pass


def deleteFileFromHDFS(hdfs_path):
    try:
        print('deleting folder in HDFS location --> {}'.format(hdfs_path))
        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-rm', '-r', hdfs_path], False)
        if err:
            print('hdfs path delete failed {}'.format(err))
        if ret:
            print('hdfs path delete success {}'.format(ret))

        return ret
    except Exception as e:
        print("error " + str(e) + " folder not good!!!")
        pass


def fetch_files_fs(task_id,**kwargs):
    task = 'init_task_{}'.format(task_id)
    ti = kwargs['ti']
    xcom_values = pullXcom(ti, task,xcom_keys)  #pull data from xcom object
    fs_filepath = xcom_values['fs_path']
    fs_pattern = xcom_values['fs_pattern']
    task_instance = kwargs['task_instance']
    # print('for fs hook'
    # fs_filepath = kwargs.get('templates_dict').get('fs_path', None)
    # fs_pattern = kwargs.get('templates_dict').get('fs_pattern', None)
    # hdfs_path = kwargs.get('templates_dict').get('hdfs_path', None)
    # hdfs_path_month = kwargs.get('templates_dict').get('hdfs_path_month', None)
    print('file path ' + fs_filepath)
    print('file pattern ' + fs_pattern)
    fs_hook = FSHook("fs_default")
    basepath = fs_hook.get_path()
    hdfs_file = ""
    full_path = "/".join([basepath, fs_filepath])
    print(full_path)
    try:
        if stat.S_ISDIR(os.stat(full_path).st_mode):
            for root, dirs, files in os.walk(full_path):
                for my_file in files:
                    if not my_file.__contains__(fs_pattern):
                        print('files to be copied to hdfs {}'.format(my_file))
                        # adding files to tha csv string
                        hdfs_file += my_file + ","
                    else:
                        print('files {}'.format(my_file))
            print('files copied to hdfs {}'.format(hdfs_file))
            # ti.xcom_push(key="fs_file", value=hdfs_file)
            # xcom_values = pullXcom(ti, task,xcom_keys)
            # print(xcom_values
            task_instance.xcom_push(key="fs_file", value=hdfs_file)
            # task_instance.xcom_push(key="fs_path", value=fs_filepath)
            # task_instance.xcom_push(key="hdfs_path", value=hdfs_path)
            # task_instance.xcom_push(key="hdfs_path_month", value=hdfs_path_month)
            return True
        else:
            # full_path was a file directly
            return True

    except OSError:
        return False
    return False


def hdfs_files_fs_copy(task_id, **kwargs):
    ti = kwargs['ti']
    task = 'init_task_{}'.format(task_id)
    xcom_values = pullXcom(ti, task,xcom_keys)  #pull data from xcom object
    # fs_file = xcom_values['fs_file']
    fs_filepath = xcom_values['fs_path']
    hdfs_path = xcom_values['hdfs_path']
    hdfs_path_month = xcom_values['hdfs_path_month']
    task = 'fetch_files_fs_{}'.format(task_id)
    fs_file = ti.xcom_pull(task_ids=task, key='fs_file')

    # print(task

    # fs_filepath = ti.xcom_pull(task_ids=task, key='fs_path')
    # hdfs_path = ti.xcom_pull(task_ids=task, key='hdfs_path')
    # hdfs_path_month = ti.xcom_pull(task_ids=task, key='hdfs_path_month')
    print("received message:{} {}".format(fs_file, fs_filepath))
    fs_hook = FSHook("fs_default")
    basepath = fs_hook.get_path()
    full_path = "/".join([basepath, fs_filepath])
    tmp_path = full_path + "/tmp/"
    # copying files to local tmp location for hdfs load.
    # if not os.path.exists(tmp_path):
    #     os.mkdir(tmp_path)
    # for file in fs_file.split(","):
    #     print(file
    #     if file != "":
    #         shutil.copy(full_path + "/" + file, tmp_path)
    # now copying files to hdfs
    # hdfs_webhook.load_file(tmp_path, hdfs_path)
    copyFileToHDFS(full_path + "/", hdfs_path_month)
    # ti.xcom_push(key='hdfs_path', value=hdfs_path)


remote_bash = """
    echo " {{ ti.xcom_pull(task_ids='init_task_vericept',key='fs_path') }}"
    echo "{{ ti.xcom_pull(task_ids='init_task_vericept',key='hdfs_path') }}"
    hdfs dfs -test -d "{{ ti.xcom_pull(task_ids='init_task_vericept',key='hdfs_path') }}"
    if [ $? == 0 ]; then
        echo "exists"
    else
        echo "dir does not exists"
    fi
    hdfs dfs -mkdir -p {{ ti.xcom_pull(task_ids='init_task_vericept',key='hdfs_path') }}
    for file in {{ ti.xcom_pull(task_ids='init_task_vericept',key='fs_path') }}/*.*
    do
        name=${file}
        if [[ $name != *".processed" ]]; then
                echo "$name"
                hdfs dfs -put -f "$name" {{ ti.xcom_pull(task_ids='init_task_vericept',key='hdfs_path') }}
        fi
    done
    
"""

remote_bash_proxy = """
    echo "{{ params.LOG_PATH }}"
    echo "{{ params.SOURCE_PATH }}"
    hdfs dfs -test -d "{{ params.LOG_PATH }}"
    if [ $? == 0 ]; then
        echo "exists"
    else
        echo "dir does not exists"
    fi
    hdfs dfs -mkdir -p {{ params.LOG_PATH }}
    for file in {{ params.SOURCE_PATH }}/*.*
    do
        name=${file}
        if [[ $name != *".processed" ]]; then
                echo "$name"
                hdfs dfs -put -f "$name" {{ params.LOG_PATH }}
        fi
    done
    
"""

def remote_file_copy(task_id, **kwargs):

    remote_bash = """
        echo "{{ ti.xcom_pull(task_ids='init_task_vericept',key='fs_path')  }}"
        echo "{{ {{ ti.xcom_pull(task_ids='init_task_vericept',key='hdfs_path')  }} }}"
        hdfs dfs -test -d "{{ params.LOG_PATH }}"
        if [ $? == 0 ]; then
            echo "exists"
        else
            echo "dir does not exists"
        fi
        hdfs dfs -mkdir -p {{ params.LOG_PATH }}
        for file in {{ params.SOURCE_PATH }}/*.*
        do
            name=${file}
            if [[ $name != *".processed" ]]; then
                    echo "$name"
                    hdfs dfs -put -f "$name" {{ params.LOG_PATH }}
            fi
        done
        
        """
    return remote_bash

def pullXcom(ti, task_id, keys):
    # type: (object, object, object) -> object
    objXcom = {}

    for key in keys.split(","):
        print("{} {}".format(key,ti.xcom_pull(key=key, task_ids=task_id)))
        objXcom[key] = ti.xcom_pull(key=key, task_ids=task_id)
    return objXcom


# def gather_metrics(task_id, **kwargs):
#
#     task = 'init_task_{}'.format(task_id)
#     ti = kwargs['ti']
#     xcom_values = pullXcom(ti, task,xcom_keys)  #pull data from xcom object
#     fs_filepath = xcom_values['fs_path']
#     source = xcom_values['source']
#     site = task_id
#     hdfs_path = xcom_values['hdfs_path']
#     hdfs_enrich_path = xcom_values['log_enrich_path'] #updated for proofpoint
#     location = xcom_values['location']
#     print("location {}".format(location)
#     push_metrics(fs_filepath, hdfs_path, hdfs_enrich_path, location, source, site)



# def push_metrics(fs_filepath, hdfs_path, hdfs_enrich_path, location, source, site):
#
#     hdfs_count = metics_files_count_hdfs(hdfs_path)
#     hdfs_enrich_count = metics_files_count_hdfs(hdfs_enrich_path)
#     hdfs_lines = metics_files_lines_hdfs(hdfs_path + "/*.*")
#     hdfs_enrich_lines = metics_files_lines_hdfs(hdfs_enrich_path + "/date*/*.*")
#
#     if not location == 'none':
#         if location == 'local':
#             files = metics_fetch_files_fs(fs_filepath)
#         else:
#             files = metics_fetch_files_ssh_fs(fs_filepath)
#         print('Files Local -> {} {} {} '.format(files['CONTENT_SIZE'], fs_filepath, files)
#
#     print('Files Count HDFS -> {} {} '.format(hdfs_path, hdfs_count)
#     print('Files Count HDFS Enrich -> {} {} '.format(hdfs_enrich_path, hdfs_enrich_count)
#     print('Files Lines HDFS -> {} {} '.format(hdfs_path, hdfs_lines)
#     print('Files Lines HDFS Enrich -> {} {} '.format(hdfs_enrich_path, hdfs_enrich_lines)
#
#     if not location == 'none':
#         pushMetices("SOPHIA.file.local", source, site, "filecount", files['CONTENT_SIZE'], sophiaConfig,
#                     "sophia_metrices_{}.log".format(str(date.today().isoformat())), False)
#     else:
#         pushMetices("SOPHIA.file.local", source, site, "filecount", 0, sophiaConfig,
#                     "sophia_metrices_{}.log".format(str(date.today().isoformat())), False)
#
#     pushMetices("SOPHIA.file.hdfs.raw", source, site, "filecount", hdfs_count['FILE_COUNT'], sophiaConfig,
#                 "sophia_metrices_{}.log".format(str(date.today().isoformat())), False)
#     pushMetices("SOPHIA.file.hdfs.raw", source, site, "linecount", hdfs_count['CONTENT_SIZE'], sophiaConfig,
#                 "sophia_metrices_{}.log".format(str(date.today().isoformat())), False)
#     pushMetices("SOPHIA.file.hdfs.enriched", source, site, "filecount", hdfs_enrich_count['FILE_COUNT'], sophiaConfig,
#                 "sophia_metrices_{}.log".format(str(date.today().isoformat())), False)
#     pushMetices("SOPHIA.file.hdfs.enriched", source, site, "linecount", hdfs_enrich_count['CONTENT_SIZE'], sophiaConfig,
#                 "sophia_metrices_{}.log".format(str(date.today().isoformat())), False)


def hdfs_files_fs_cleanup(task_id, **kwargs):
    ti = kwargs['ti']
    task = 'hdfs_file_cleanup_task_{}'.format(task_id)
    fs_file = ti.xcom_pull(task_ids=task, key='fs_file')
    fs_filepath = ti.xcom_pull(task_ids=task, key='fs_path')
    hdfs_path = ti.xcom_pull(task_ids=task, key='hdfs_path')

    deleteFileFromHDFS(hdfs_path)

    fs_hook = FSHook("fs_default")
    basepath = fs_hook.get_path()
    full_path = fs_filepath

    print(full_path)
    print(fs_filepath)

    for file in fs_file.split(","):
        print(file)
        if file != "":
            src = full_path + "/" + file
            dest = full_path + "/" + file + ".processed"
            print("moving file {} --> {}".format(src, dest))
            shutil.move(src, dest)
    print('Clean up success')

def getDate(**kwargs):
    try:
        if kwargs.get('params').get('date', None) is None:
            print('No Date Param for overide')
            yesterday = date.today() - timedelta(1)
            YEAR = "{:04d}".format(yesterday.year)
            MONTH = "{:02d}".format(yesterday.month)
            DAY = "{:02d}".format(yesterday.day)
        else:
            print('Date Param Avaliable for overide' + kwargs.get('params').get('date', None))
            (YEAR, MONTH, DAY) = str(kwargs.get('params').get('date', None)).split("-")

    except KeyError:
        yesterday = date.today() - timedelta(1)
        YEAR = "{:04d}".format(yesterday.year)
        MONTH = "{:02d}".format(yesterday.month)
        DAY = "{:02d}".format(yesterday.day)

    return YEAR, MONTH, DAY




def sshoperator(ssh_conn_id, ssh_hook=None, remote_host=None, command=None, timeout=10, do_xcom_push=None, **kwargs):
    """

    :param ssh_conn_id:
    :param ssh_hook:
    :param remote_host:
    :param command:
    :param timeout:
    :param do_xcom_push:
    :param kwargs:
    :return:
    """
    try:

        if ssh_conn_id and not ssh_hook:
            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

        if not ssh_hook:
            raise AirflowException("can not operate without ssh_hook or ssh_conn_id")

        if remote_host is not None:
            ssh_hook.remote_host = remote_host

        if not command:
            raise AirflowException("no command specified so nothing to execute here.")


        #access the task
        if kwargs['task_id']:
            task = 'init_task_{}'.format(kwargs['task_id'])
        else:
            task = 'boot_task'
        ti = kwargs['ti']
        #get all the parameters
        xcom_keys = kwargs['params']
        xcom_values = pullXcom(ti, task,xcom_keys)
        LOG_PATH = xcom_values['hdfs_path']
        SOURCE_PATH = xcom_values['fs_path']
        log.info(str(command))
        tmp_cmd = str(command).replace("{{ params.LOG_PATH }}", LOG_PATH)
        tmp_cmd = tmp_cmd.replace("{{ params.SOURCE_PATH }}", SOURCE_PATH)
        log.info(tmp_cmd)
        command = tmp_cmd

        # Auto apply tty when its required in case of sudo
        get_pty = False
        if command.startswith('sudo'):
            get_pty = True

        ssh_client = ssh_hook.get_conn()

        # set timeout taken as params
        stdin, stdout, stderr = ssh_client.exec_command(command=command,
                                                        get_pty=get_pty,
                                                        timeout=timeout
                                                        )
        # get channels
        channel = stdout.channel

        # closing stdin
        stdin.close()
        channel.shutdown_write()

        agg_stdout = b''
        agg_stderr = b''

        # capture any initial output in case channel is closed already
        stdout_buffer_length = len(stdout.channel.in_buffer)

        if stdout_buffer_length > 0:
            agg_stdout += stdout.channel.recv(stdout_buffer_length)

        # read from both stdout and stderr
        while not channel.closed or \
                channel.recv_ready() or \
                channel.recv_stderr_ready():
            readq, _, _ = select([channel], [], [], timeout)
            for c in readq:
                if c.recv_ready():
                    line = stdout.channel.recv(len(c.in_buffer))
                    line = line
                    agg_stdout += line
                    log.info(line.decode('utf-8').strip('\n'))
                if c.recv_stderr_ready():
                    line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                    line = line
                    agg_stderr += line
                    log.warning(line.decode('utf-8').strip('\n'))
            if stdout.channel.exit_status_ready() \
                    and not stderr.channel.recv_stderr_ready() \
                    and not stdout.channel.recv_ready():
                stdout.channel.shutdown_read()
                stdout.channel.close()
                break

        stdout.close()
        stderr.close()

        exit_status = stdout.channel.recv_exit_status()
        if exit_status is 0:
            # returning output if do_xcom_push is set
            if do_xcom_push:
                enable_pickling = configuration.conf.getboolean(
                    'core', 'enable_xcom_pickling'
                )
                if enable_pickling:
                    return agg_stdout
                else:
                    return b64encode(agg_stdout).decode('utf-8')

        else:
            error_msg = agg_stderr.decode('utf-8')
            raise AirflowException("error running cmd: {0}, error: {1}"
                                   .format(command, error_msg))
    except Exception as e:
        raise AirflowException("SSH operator error: {0}".format(str(e)))

    return True

    print("ssh call")

    return True


def sshoperator_remote(ssh_conn_id, ssh_hook=None, remote_host=None, command=None, timeout=10, do_xcom_push=None, **kwargs):

    if ssh_conn_id and not ssh_hook:
        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    get_pty = False
    if command.startswith('sudo'):
        get_pty = True


    kwargs['local_path']

    ssh_client = ssh_hook.get_conn()


    # set timeout taken as params
    stdin, stdout, stderr = ssh_client.exec_command(command=command,
                                                    get_pty=get_pty,
                                                    timeout=timeout
                                                    )
    # get channels
    channel = stdout.channel

    # closing stdin
    stdin.close()
    channel.shutdown_write()

    agg_stdout = b''
    agg_stderr = b''

    # capture any initial output in case channel is closed already
    stdout_buffer_length = len(stdout.channel.in_buffer)

    if stdout_buffer_length > 0:
        agg_stdout += stdout.channel.recv(stdout_buffer_length)

    # read from both stdout and stderr
    while not channel.closed or \
            channel.recv_ready() or \
            channel.recv_stderr_ready():
        readq, _, _ = select([channel], [], [], timeout)
        for c in readq:
            if c.recv_ready():
                line = stdout.channel.recv(len(c.in_buffer))
                line = line
                agg_stdout += line
                log.info(line.decode('utf-8').strip('\n'))
            if c.recv_stderr_ready():
                line = stderr.channel.recv_stderr(len(c.in_stderr_buffer))
                line = line
                agg_stderr += line
                log.warning(line.decode('utf-8').strip('\n'))
        if stdout.channel.exit_status_ready() \
                and not stderr.channel.recv_stderr_ready() \
                and not stdout.channel.recv_ready():
            stdout.channel.shutdown_read()
            stdout.channel.close()
            break

    stdout.close()
    stderr.close()
    return agg_stdout

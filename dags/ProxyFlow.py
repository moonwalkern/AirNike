import logging
from datetime import datetime, timedelta

from DruidIndexUtils import DruidIndexTask, copyToDeltaDir
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.contrib.sensors.hdfs_sensor import HdfsSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

from PhoenixUtils import startPythonBatch
from HdfsUtils import fetch_files_fs, hdfs_files_fs_copy, getDate, sshoperator, remote_bash_proxy

log = logging.getLogger(__name__)

import softBugConfig
import proxyschema

default_args = {
    'owner': 'scaledata',
    'start_date': datetime(2018, 8, 26),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'email': ['info@scaledata.biz'],
    'email_on_failure': True
}

softbugConfig = softBugConfig.SOFTBUG_CONFIGS['ProxyConfigs']
LOCAL_PATH = softbugConfig['raw_data_path']  # '/rawdata/src/'
config = softbugConfig['spark_config']
jars = softBugConfig.SOFTBUG_CONFIGS['jars']


EXECUTABLE_PATH = softBugConfig.SOFTBUG_CONFIGS['executableJarfile']
OTHER_PARAM_OVERRIDES = 'offset=1'  # sys.argv[4]
ENRICHED_DIR = "/user/user/source/*/enriched/"
# ENRICHED_DIR = "/user/user/source/enriched/*/"

dag = DAG('ProxyFlow', description='ProxyFlow for multiple sites',
          schedule_interval='0 8 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


def final_task():
    print('Final Task')

CONFIGS = softbugConfig['sites']

xcom_keys = 'source,fs_path,hdfs_path,hdfs_path_month,log_enrich_path,log_enrich_path_phoenix,log_iplookup_path,glo_path,fs_pattern,params'

def bootstrap(**kwargs):
    print('Initialization of Parameters')
    print(kwargs)

    (YEAR, MONTH, DAY) = getDate(**kwargs)
    HDFS_DATA_PATH = softbugConfig["hdfs_data_path"]
    ENRICHED_DIR = "/user/scorpion/data/proxy/*/enriched/"

    kwargs['ti'].xcom_push(key='date', value=str(YEAR + MONTH + DAY))
    kwargs['ti'].xcom_push(key='index_template', value='/home/scorpion/airflow/dags/index_templates/proxy_index_template.json')
    kwargs['ti'].xcom_push(key='log_enrich_path', value=ENRICHED_DIR)
    kwargs['ti'].xcom_push(key='source', value='proxy')

def init_fun(task_id, **kwargs):
    print('Initialization of Parameters')
    print(kwargs)

    # Define YEAR MONTH DAY Values
    # when date it passed a an argument for manual Airflow initiation using the airflow trigger_dag <dag_id> --conf {} command
    # the date will be available in the kwarg dict
    # First check if the object/key is available if available then use that as the event date YEAR MONTH DAY
    # if not available used the current date from and caculate D-1 as event date YEAR MONTH DAY

    # try:
    #     if kwargs.get('params').get('date', None) is None:
    #         print('No Date Param for overide'
    #         yesterday = date.today() - timedelta(1)
    #         YEAR = "{:04d}".format(yesterday.year)
    #         MONTH = "{:02d}".format(yesterday.month)
    #         DAY = "{:02d}".format(yesterday.day)
    #     else:
    #         print('Date Param Avaliable for overide' + kwargs.get('params').get('date', None)
    #         (YEAR, MONTH, DAY) = str(kwargs.get('params').get('date', None)).split("-")
    #
    # except KeyError:
    #     yesterday = date.today() - timedelta(1)
    #     YEAR = "{:04d}".format(yesterday.year)
    #     MONTH = "{:02d}".format(yesterday.month)
    #     DAY = "{:02d}".format(yesterday.day)
    (YEAR, MONTH, DAY) = getDate(**kwargs)
    print("Date ---> {}/{}/{}".format(YEAR,MONTH,DAY))
    LOCAL_PATH = softbugConfig['raw_data_path']
    HDFS_DATA_PATH = softbugConfig["hdfs_data_path"]
    # Building the params.
    # SOURCE_PATH = LOCAL_PATH + "{}/{}/{}/{}".format(sites['site_name'], YEAR, MONTH, DAY)
    # LOG_PATH = LOCAL_PATH + "{}/{}/{}/{}/{}".format(task_id,'rawdata', YEAR, MONTH, DAY)
    # LOG_PATH_TMP = LOCAL_PATH + "{}/{}/{}/{}".format(task_id,'rawdata', YEAR, MONTH)
    #
    # GLO_PATH = "/user/user/ldap/enriched/"
    # LOG_ENRICH_PATH = LOG_PATH.replace("rawdata", "enriched")
    # LOG_ENRICH_PATH_PHOENIX = LOG_ENRICH_PATH.replace("enriched", "enriched/phoenix")

    SOURCE_PATH = LOCAL_PATH + "{}/{}/{}/{}/{}".format(task_id,'rawdata', YEAR, MONTH, DAY)
    # SOURCE_PATH = LOCAL_PATH + "{}/{}/{}/{}".format(task_id, YEAR, MONTH, DAY)
    LOG_PATH = HDFS_DATA_PATH + "{}/{}/{}/{}/{}".format(task_id, 'rawdata', YEAR, MONTH, DAY)
    LOG_PATH_TMP = HDFS_DATA_PATH + "{}/{}/{}/{}".format(task_id, 'rawdata', YEAR, MONTH)
    LOG_ENRICH_PATH = LOG_PATH.replace("rawdata", "enriched")
    GLO_PATH = '/data/hru/20190101/'
    LOG_ENRICH_PATH_PHOENIX = LOG_ENRICH_PATH.replace("enriched", "enriched/phoenix")
    LOG_IPLOOKUP_PATH = LOG_ENRICH_PATH.replace("enriched", "enriched/ip")
    ENRICHED_DIR_DELTA = "/user/user/usb/{0}/enriched/".format(task_id)
    sites = softbugConfig['sites'][task_id]
    SCHEMA = sites['schema']
    LOCATION = sites['location']
    if CONFIGS[task_id].has_key('magic_space'):
        DUMB_SCHEMA = SCHEMA[:]
        DUMB_SCHEMA.insert(SCHEMA.index("sc_status"), "empty")
        SCHEMA = DUMB_SCHEMA

    if CONFIGS[task_id].has_key('new_schema'):
        DUMB_SCHEMA = proxyschema.BASIC_B_V3
        SCHEMA = DUMB_SCHEMA

    PARAMS = dict({
                      'log.source.name': 'proxy',
                      'log.path': LOG_PATH,
                      'log.schema': SCHEMA,
                      'log.userlookup.path': '/user/hdfs/userdata/all',
                      'log.glo.path': GLO_PATH,
                      'log.delimiter.enrich': 'tsv',
                      'log.header.enrich': 'false',
                      'log.phoenix.table': 'false',
                      'log.phoenix.create': 'false',
                      'log.phoenix.tablename': 'proxy',
                      'log.zookeeper.quorum': '192.168.60.102:2181',
                      'log.site.name': task_id,
                      'log.path.enrich': LOG_ENRICH_PATH,
                      'log.path.phoenix': LOG_ENRICH_PATH_PHOENIX,
                      'log.iplookup.path': LOG_IPLOOKUP_PATH,
                      'log.date': "{0}{1}{2}".format(YEAR, MONTH, DAY),
                      'log.monitoring.endpoint': 'http://soctxcdev01.pmc.cmp.com:8080/monitoring',
                      'log.columns.add': 'c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,m1,m2,m3,m4,m5,m6',
                      'log.kafka.topic': 'druid-kafka-proxy',
                      'log.kafka.create': 'false',
                      'log.kafka.servers': 'soctxadev01.pmc.cmp.com:6667',
                      'log.rowcase.headers': 'vzid:lower,x_cs_auth_domain:upper,x_exception_id:lower,sc_filter_result:uppper,cs_referer:lower,sc_status:upper,s_action:upper,cs_method:upper,rs_content_type:lower,cs_uri_scheme:lower,cs_host:lower,cs_uri_path:lower,cs_uri_query:lower,cs_uri_extension:lower,cs_user_agent:lower,x_bluecoat_application_name:lower,x_bluecoat_application_operation:lower,cs_categories:lower,cs_auth_group:lower,company_name:lower,x_virus_id:upper',
                      'log.partition.by.date': 'true',
                      'log.number.partitions': 1000,
                      'log.save.data': 'true',
                      'log.save.using.quote': 'false',
                      'log.save.header': 'false'
                  }.items() + [v.split("=") for v in OTHER_PARAM_OVERRIDES.split(",")])

    PARAMS['log.schema'] = ','.join(PARAMS['log.schema'])
    print(PARAMS)

    kwargs['ti'].xcom_push(key='date', value=str(YEAR + MONTH + DAY))
    kwargs['ti'].xcom_push(key='fs_path', value=SOURCE_PATH)
    kwargs['ti'].xcom_push(key='hdfs_path', value=LOG_PATH)
    kwargs['ti'].xcom_push(key='hdfs_path_month', value=LOG_PATH_TMP)
    kwargs['ti'].xcom_push(key='log_enrich_path', value=LOG_ENRICH_PATH)
    kwargs['ti'].xcom_push(key='log_enrich_path_phoenix', value=LOG_ENRICH_PATH_PHOENIX)
    kwargs['ti'].xcom_push(key='log_iplookup_path', value=LOG_IPLOOKUP_PATH)
    kwargs['ti'].xcom_push(key='glo_path', value=GLO_PATH)
    kwargs['ti'].xcom_push(key='fs_pattern', value=".processed")
    kwargs['ti'].xcom_push(key='params', value=['{0}={1}'.format(k, v) for (k, v) in PARAMS.iteritems()])
    kwargs['ti'].xcom_push(key='param', value=[PARAMS])
    kwargs['ti'].xcom_push(key='index_template', value='/home/user/airflow/dags/index_templates/proxy_index_template.json')
    kwargs['ti'].xcom_push(key='enriched_dir_delta', value=ENRICHED_DIR_DELTA)
    kwargs['ti'].xcom_push(key='source', value='proxy')
    kwargs['ti'].xcom_push(key='table', value='proxy')
    kwargs['ti'].xcom_push(key='location', value=LOCATION)
    return [PARAMS]

def load_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@daily",
    )

    with dag_subdag:
        for site in CONFIGS:
            sites = softbugConfig['sites'][site]

            t4 = HdfsSensor(task_id='hdfs_sensor_task_{0}'.format(site),
                            hdfs_conn_id='hdfs_conn',
                            filepath="{{ ti.xcom_pull(task_ids='init_task_" + site + "',key='hdfs_path') }}",
                            poke_interval=30,
                            dag=dag_subdag)

            t5 = SparkSubmitOperator(
                task_id='spark_submit_job_{0}'.format(site),
                conn_id='spark_default',
                java_class='com.scaledata.softbug.datasources.proxy.ProxyBlueCoat',
                application=EXECUTABLE_PATH,
                application_args=["{{ti.xcom_pull(task_ids='init_task_" + site + "')}}"],
                # application_args=["{{ti.xcom_pull(task_ids='hdfs_file_task')}}"],
                total_executor_cores='8',
                executor_cores='8',
                executor_memory='5G',
                num_executors='70',
                name='spark-airflow-phoenix',
                verbose=True,
                driver_memory='5G',
                xcom_push='true',
                jars=jars,
                conf=config,
                dag=dag_subdag,
                retries=1
            )

            # t8 = PythonOperator(task_id='phoenix_batch_task_{0}'.format(site),
            #                     python_callable=startPythonBatch,
            #                     op_kwargs={'task_id': site},
            #                     provide_context=True,
            #                     dag=dag_subdag)

            # t9 = PythonOperator(task_id='gather_metrics_{0}'.format(site),
            #                     python_callable=gather_metrics,
            #                     op_kwargs={'task_id': site},
            #                     provide_context=True,
            #                     retries=0,
            #                     dag=dag_subdag)
            init = PythonOperator(task_id='init_task_{0}'.format(site),
                                  python_callable=init_fun,
                                  op_kwargs={'task_id': site},
                                  provide_context=True,
                                  templates_dict={'fs_path': '{{ params.date }}', 'hdfs_path': 'LOG_PATH',
                                                  'hdfs_path_month': 'LOG_PATH_TMP',
                                                  'fs_pattern': ".processed"},
                                  dag=dag_subdag)
            if sites['location'] == 'local':
                t1 = FileSensor(task_id='file_sensor_task_{0}'.format(site),
                                filepath="{{ ti.xcom_pull(task_ids='init_task_" + site + "',key='fs_path') }}",
                                fs_conn_id="fs_default",
                                poke_interval=30, soft_fail=True, timeout=2, dag=dag_subdag)
                t2 = PythonOperator(task_id='fetch_files_fs_{0}'.format(site),
                                    python_callable=fetch_files_fs,
                                    op_kwargs={'task_id': site},
                                    provide_context=True,
                                    dag=dag_subdag)
                t3 = PythonOperator(task_id='hdfs_files_fs_{0}'.format(site),
                                    python_callable=hdfs_files_fs_copy,
                                    op_kwargs={'task_id': site},
                                    provide_context=True,
                                    dag=dag_subdag)

                init >> t1 >> t2 >> t3 >> t4 >> t5
            else:

                # t1 = SSHOperator(ssh_conn_id='ssh_default', task_id='remote_{}'.format(site), command=remote_bash_source,
                #                  params={'SOURCE_PATH': "{{ ti.xcom_pull(task_ids='init_task_" + site + "',key='fs_path') }}",
                #                          'LOG_PATH': "{{ ti.xcom_pull(task_ids='init_task_" + site + "',key='hdfs_path') }}"},
                #                  dag=dag_subdag)
                t1 = PythonOperator(task_id='remote_{}'.format(site),
                                    python_callable=sshoperator,
                                    op_kwargs={'task_id': site,'ssh_conn_id': 'ssh_default', 'command': remote_bash_proxy,
                                               'params':'fs_path,hdfs_path'},
                                    provide_context=True,
                                    dag=dag_subdag)

                init >> t1 >> t4 >> t5

    return dag_subdag


load_tasks = SubDagOperator(
    task_id='load_tasks',
    subdag=load_subdag('ProxyFlow',
                       'load_tasks', default_args),
    default_args=default_args,
    dag=dag,
)

# druid_task = PythonOperator(task_id='druid_indexing_task',
#                             python_callable=druid_indexing,
#                             provide_context=True,
#                             templates_dict={
#                                 'index_template': '/home/user/airflow/dags/index_templates/source_index_template.json',
#                                 'site': 'source'},
#                             dag=dag
#                             )

# move_delta_files = PythonOperator(task_id='move_delta_files',
#                                   python_callable=copyToDeltaDir,
#                                   provide_context=True,
#                                   # templates_dict={'datestr': str(YEAR + MONTH + DAY),
#                                   #                 'enriched_dir_delta': ENRICHED_DIR},
#                                   dag=dag)
druid_index_file_task = PythonOperator(task_id='druid_indexing_task',
                                       python_callable=DruidIndexTask,
                                       provide_context=True,
                                       # templates_dict={
                                       #     'index_template': '/home/user/airflow/dags/index_templates/source_index_template.json',
                                       #     'source': 'source',
                                       #     'site': 'source',
                                       #     'date': str(YEAR + MONTH + DAY),
                                       #     'enrichedDir': ENRICHED_DIR
                                       # },
                                       retries=3,
                                       dag=dag)

final = PythonOperator(task_id='final_task',
                       python_callable=final_task,
                       trigger_rule=TriggerRule.ALL_DONE,
                       dag=dag)

dummy_operator = DummyOperator(task_id='dummy_task',
                               retries=3,
                               dag=dag)


bootstrap = PythonOperator(task_id='boot_task',
                           python_callable=bootstrap,
                           provide_context=True,
                           dag=dag)

dummy_operator >> bootstrap >> load_tasks >> final >> druid_index_file_task

# >> druid_task


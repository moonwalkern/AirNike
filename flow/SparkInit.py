from datetime import datetime, timedelta

from airflow import DAG
# from airflow.contrib.operators.phoenix_batch_operator import PhonixBatchOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'sreeji',
    'start_date': datetime(2018, 8, 26),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('SparkInit', description='StarterInit',
          schedule_interval='* * * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

PATH = '/user/hdfs/userlist/'
EXECUTABLE_PATH = '/home/hdfs/new_platform/lib/spark-druid-etl-0.1.0-jar-with-dependencies.jar'  # sys.argv[3]
OTHER_PARAM_OVERRIDES = 'offset=1'  # sys.argv[4]
SITE = 'twnoh'
# PATH = 'file:////Users/gopalsr/Documents/Sreeji/code/etl_data_testing/data-new/user_list/'
# EXECUTABLE_PATH = '/Users/gopalsr/Documents/Sreeji/code/spark-druid-etl/target/spark-druid-etl-0.1.0-jar-with-dependencies.jar'  # sys.argv[3]

config = {
    'queue': 'default',
}

BASIC_B = [
    "date",
    "time",
    "time_taken",
    "c_ip",
    "cs_username",
    "cs_auth_group",
    "x_exception_id",
    "sc_filter_result",
    "cs_categories",
    "cs_referer",
    "sc_status",
    "s_action",
    "cs_method",
    "rs_content_type",
    "cs_uri_scheme",
    "cs_host",
    "cs_uri_port",
    "cs_uri_path",
    "cs_uri_query",
    "cs_uri_extension",
    "cs_user_agent",
    "s_ip",
    "sc_bytes",
    "cs_bytes",
    "x_virus_id"
]

BASIC_B_V0 = BASIC_B + [
    "x_bluecoat_application_name",
    "x_bluecoat_application_operation",
    "r_ip"
]

BASIC_B_V1 = BASIC_B + [
    "x_bluecoat_application_name",
    "x_bluecoat_application_operation",
    "x_bluecoat_transaction_uuid",
    "x_icap_reqmod_header",
    "x_icap_respmod_header"
]

BASIC_B_V2 = BASIC_B[0:6] + [
    "s_supplier_name",
    "s_supplier_ip",
    "s_supplier_country",
    "s_supplier_failures"
] + BASIC_B[6:] + [
                 "x_bluecoat_application_name",
                 "x_bluecoat_application_operation",
                 "cs_threat_risk",
                 "x_bluecoat_transaction_uuid",
                 "x_icap_reqmod_header",
                 "x_icap_respmod_header"
             ]

BASIC_B_V3 = [
                 "localtime"
             ] + BASIC_B_V2[2:5] + [
                 "x_cs_auth_domain",
                 "x_auth_credential_type"
             ] + BASIC_B_V2[5:] + [
                 "r_ip"
             ]

# timestamp	datetime
# sc-result-code	action	action
# sc-http-status	 sc-status	sc_status
# sc-bytes	 sc-bytes	bytes_in
# cs-method	 http_method	cs_method
# s-hierarchy	??????	??????
# cs_username	src_user	src_user
# c-ip	src_ip	src_ip
# c-port	??????	??????
# s-computerName	dest_host	dst_host
# cs-mime-type	http_content_type	content_type
# cs-bytes	bytes_out	bytes_out
# s-ip	dest_ip	dst_ip
# s-port	 cs-uri-port	dst_port
# cs-auth-group	cs_auth_group	auth_group
# x-webcat-code-full	category	category
# cs(User-Agent)	http_user_agent	cs_user_agent
# cs-url	 cs_uri_scheme dest_host cs_uri_path	cs_uri_scheme dest_host cs_uri_port cs_uri_path cs_uri_query cs_uri_extension
# cs-uri	 cs_uri_path	cs_uri_path




BASIC_V = [
    "timestamp",
    "s_action",
    "sc_status",
    "sc_bytes",
    "cs_method",
    "s_hierarchy",
    "cs_username",
    "c_ip",
    "c_port",
    "cs_host",
    "rs_content_type",
    "cs_bytes",
    "s_ip",
    "cs_uri_port",
    "cs_auth_group",
    "cs_categories",
    "cs_user_agent",
    "cs_url",
    "cs_uri"
]

BASIC_V = [
    "timestamp",
    "s_action",
    "sc_status",
    "sc_bytes",
    "cs_method",
    "s_hierarchy",
    "cs_username",
    "c_ip",
    "c_port",
    "cs_host",
    "rs_content_type",
    "cs_bytes",
    "s_ip",
    "cs_uri_port",
    "cs_auth_group",
    "cs_categories",
    "cs_user_agent",
    "cs_url",
    "cs_uri"
]

CONFIGS = {
    'ashva': {
        'schema': BASIC_B
    },
    'indmo': {
        'schema': BASIC_B
    },
    'orgny': {
        'schema': BASIC_B_V1,
        'new_schema': ['orbgnyaei-ipx']
    },
    'twnoh': {
        'schema': BASIC_B_V1,
        'new_schema': ['twbgohaai-ipx']
    },
    'dbloh': {
        'schema': BASIC_B_V0,
        'magic_space': ['ohdblndbcproxy11_main__80']
    },
    'fldmd': {
        'schema': BASIC_B_V1,
        'new_schema': ['faldmdfli-ipx']
        # 'magic_space': ['fdc-bcoat-{1,2,4}_GSOCLogFeed']
    },
    'sacca': {
        'schema': BASIC_B_V1,
        'new_schema': ['scrmcagni-ipx']
        # 'magic_space': ['sac-bcoat-{1,2,3}_GSOCLogFeed']
    },
    'frhnj': {
        'schema': BASIC_B_V1,
        # 'new_schema': ['*']
        'new_schema': ['frhdnjbbi-ipx']
        # 'magic_space': ['fh-bcoat-{4,5}_GSOCLogFeed']
    },
    'irvtx': {
        'schema': BASIC_B_V1,
        'new_schema': ['irngtxbsn-ipx']
        # 'new_schema': ['*-irngt.gz']
        # 'magic_space': ['irv-bcoat-{1,2}_GSOCLogFeed']
    },
    'tpafl': {
        'schema': BASIC_B_V1,
        'new_schema': ['tmtrflaai-ipx'],
        # 'magic_space': ['tpa-bcoat-{1,2}_GSOCLogFeed']
    },
    'vzbap': {
        'schema': BASIC_B_V2,
        'magic_space': [
            'SG_proxyhk2_main_GSOC__2',
            'SG_proxysg1_main_GSOC__4'
        ]
    }
}

config = {
    'queue': 'default',
}

CONFIG = CONFIGS[SITE]
SCHEMA = ",".join(CONFIG['schema'])
# SCHEMA = CONFIG['schema']

PARAMS = dict({
                  'log.source.name': 'proxy',
                  'log.path': PATH,
                  'log.schema': SCHEMA,
                  'log.userlookup.path': '/user/hdfs/userdata/all',
                  'log.glo.path': '',
                  'log.delimiter.enrich': 'tsv',
                  'log.header.enrich': 'false',
                  'log.phoenix.table': 'true',
                  'log.phoenix.create': 'true',
                  'log.phoenix.tablename': 'proxy',
                  'log.zookeeper.quorum': '192.168.60.102:2181',
                  'log.site.name': SITE,
                  'log.path.enrich': '',
                  'log.monitoring.endpoint': 'http://soctxcdev01.gsoc.verizon.com:8080/monitoring',
                  'log.columns.add': 'c0,c1,c2,c3,c4,c5,m1,m2,m3',
                  'log.kafka.topic': 'druid-kafka-proxy',
                  'log.kafka.create': 'true',
                  'log.kafka.servers': 'soctxadev01.gsoc.verizon.com:6667',
                  'log.rowcase.headers': 'cs_username:lower,vzid:lower,x_cs_auth_domain:upper,x_exception_id:lower,sc_filter_result:uppper,cs_referer:lower,sc_status:upper,s_action:upper,cs_method:upper,rs_content_type:lower,cs_uri_scheme:lower,cs_host:lower,cs_uri_path:lower,cs_uri_query:lower,cs_uri_extension:lower,cs_user_agent:lower,x_bluecoat_application_name:lower,x_bluecoat_application_operation:lower,cs_categories:lower,cs_auth_group:lower',
                  'log.partition.by.date': 'true'
              }.items() + [v.split("=") for v in OTHER_PARAM_OVERRIDES.split(",")])


# spark_submit_task = SparkSubmitOperator(
#     task_id='spark_submit_job',
#     conn_id='spark_default',
#     java_class='com.verizon.gsoc.datasources.phoenix.Phoenix',
#     application=EXECUTABLE_PATH,
#     # application_args=[' '.join(['{0}={1}'.format(k, v) for (k, v) in PARAMS.iteritems()])],
#     application_args=['{0}={1}'.format(k, v) for (k, v) in PARAMS.iteritems()],
#     total_executor_cores='1',
#     executor_cores='1',
#     executor_memory='2g',
#     num_executors='2',
#     name='spark-airflow-phoenix',
#     verbose=True,
#     driver_memory='1g',
#     xcom_push='true',
#     conf=config,
#     dag=dag,
# )


def print_hello():
    return 'Finally it worked!!!!' + str(datetime.now().strftime("%m%d%Y-%H%M"))


def print_check():
    return 'Finally it worked!!!!' + str(datetime.now().strftime("%m%d%Y-%H%M"))


# phoenix_batch_task = PhonixBatchOperator(phoenix_batch_operator_param='This is phoenix operator',
#                                          task_id='phoenix_batch_task', dag=dag)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

spark_success = PythonOperator(task_id='spark_success_task', python_callable=print_hello, dag=dag)

dummy_operator >> spark_success

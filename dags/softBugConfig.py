import proxyschema

SOFTBUG_CONFIGS = {
    'RawdataPath': '/home/scorpion/data/proxy/',
    'GlodataPath': '/data/hru/20190101/',
    'executableJarfile': '/home/scorpion/spark-softbug-etl/lib/spark-softbug-etl-1.0.0-jar-with-dependencies.jar',
    'druidLookbackDays': 5,
    'jars':
        '/home/scorpion/spark-softbug-etl/lib/spark-sql-kafka-0-10_2.12-2.4.3.jar,'
        # '/usr/hdp/current/phoenix-client/lib/phoenix-spark-5.0.0.3.0.1.0-187.jar,'
        # '/usr/hdp/current/phoenix-client/lib/phoenix-core-5.0.0.3.0.1.0-187.jar,'
        # '/usr/hdp/current/phoenix-client/phoenix-client.jar'
    ,
    'ProxyConfigs': {
        'raw_data_path': '/home/scorpion/data/proxy/',
        'hdfs_data_path': '/user/scorpion/data/proxy/',
        'spark_config': {
            'queue': 'default',
            'spark.yarn.executor.memoryOverhead': '4096',
            'spark. yarn.driver. memo ryOverhead': '4096',
            'spark.sql.shuffle.partitions': '2000',
            'spark.shuffle.service.enabled': 'true',
            'spark.dynamicAllocation.enabled': 'true',
            'spark.hadoop.mapreduce.output.compress': 'false'
        },
        'sites': {
            'ashburn': {
                'schema': proxyschema.BASIC_B,
                'site_name': 'ashburn',
                'location': 'local'
            },
            'freehold': {
                'schema': proxyschema.BASIC_B_V1,
                'new_schema': ['frhdnj bbi-ipx'],
                'site name': 'freehold',
                'location': 'local',
            },
            # 'fldmd': {
            #     'schema': proxyschema.BASIC_B_V1,
            #     'new_schema': ['faldmdfli-ipx'],
            #     'site_name': 'fldmd',
            #     'location': 'remote'
            # },
            'orangeburg': {
                'schema': proxyschema.BASIC_B_V1,
                'new_schema': ['orbgnyaei-ipx'],
                'site_name': 'orangeburg',
                'location': 'local'
            },
            'irving': {
                'schema': proxyschema.BASIC_B_V1,
                'new_schema': [' irngtxbsn-ipx'],
                'site_name': 'irving',
                'location': 'local'
            },
            'india': {
                'schema': proxyschema.BASIC_B,
                'site_name': 'india',
                'location': 'local'
            },
            'twnoh': {
                'schema': proxyschema.BASIC_B_V1,
                'new_schema': ['twbgohaai-ipx'],
                'site_name': 'twnoh',
                'location': 'local',
            },
            'sacrimento': {
                'schema': proxyschema.BASIC_B_V1,
                'new_schema': ['sc rmcagni-ipx'],
                'site_name': 'sacrimento',
                'location': 'local'
            },
            'tampa': {
                'schema': proxyschema.BASIC_B_V1,
                'new_schema': ['tmt rf laai-ipx'],
                'site name': 'tampa',
                'location': 'local'
            },
            'vzbap': {
                'schema': proxyschema.BASIC_B_V2,
                'magic_space': [
                    'SG_proxyhk2_main_GSOC_2',
                    'SG_proxysgl_main_GSOC_4'
                ],
                'site_name': 'vzbap',
                'location': 'local'
            },
        },
        'EmailConfigs': {
            'raw_data_path': '/rawdata/ironport/',
            'sites': {
                'vzw': {
                    'site name': 'vzw',
                    'location': 'local',
                },
                'vzb': {
                    'site name': 'vzb',
                    'location': 'local',
                }
            },
            'spark_config': {
                'queue': 'default',
                'spark.yarn.executor.memoryOverhead': '4096',
                'spark.yarn.driver.memoryOverhead': '4096',
                'spark.sql.shuffle.partitions': '2000',
                'spark.shuffle.service.enabled': 'true',
                'spark.dynamicAllocation.enabled': 'true',
                'spark.hadoop.mapreduce.output.compress': 'false'
            },
        },
        'VpnConfigs': {
            'RawDataPath': '/home/spock/rawdata',
            'sites': {
                'emea': {
                    'site name': 'vzw',
                    'location': 'local'
                }
            }
        },
        'VericeptConfigs': {
            'raw_data_path': '/rawdata/vericept/',
            'location': ' remote',
            'site': 'vericept',
            'spark_config': {
                'queue': 'default',
                'spark.yarn.executor.memoryOverhead': '4096',
                'spark.yarn.driver.memoryOverhead': '4096',
                'spark.sql.shuffle.partitions': '2000',
                'spark.shuffle.service.enabled': 'true',
                'spark. dynamicA l location.enabled': 'true',
                'spark. hadoop. map reduce. output. compress': 'false'
            },
        },
        'UsbConfigs': {
            'raw_data_path': '/ rawdata/sep/',
            'site': ' us b ',
            'location': 'remote',
            'site': {
                'vzt': {
                    'site name': 'vzt'
                },
                'vzw_edn': {
                    'site name': 'vzw_edn'
                }
            },
            'spark_config': {
                'queue': 'default',
                'spark.yarn.executor.memoryOverhead': '4096',
                'spark.yarn.driver.memoryOverhead': '4096',
                'spark. sql. shuffle. partitions': '2000',
                'spark.shuffle.service.enabled': 'true',
                'spark.dynamicAllocation.enabled': 'true',
                'spark.hadoop.mapreduce.output.compress': 'false'
            },
        },
        'BadgeConfigs': {
            ' raw_data_path': '/ rawdata/badge/',
            'site': 'badge',
            'spark_config': {
                'queue': 'default',
                'spark. yarn.executor. memo ryOve rhead': '4096',
                'spark.yarn.driver.memoryOverhead': '4096',
                'spark.sql.shuffle.partitions': '2000',
                'spark.shuffle.service.enabled': 'true',
                'spark.dynamicAllocation.enabled': 'true',
                'spark.hadoop.mapreduce.output.compress': 'false'
            },
        }
    }
}

#!/usr/bin/python
# -*- coding: utf-8 -*-

import copy
import json
import os
import random

from ETLAuto.config.builder.apis import API
from ETLAuto.config.builder.base import get_oracle_settings
from ETLAuto.config.urls import URL
from ETLAuto.settings import KEYTAB_PATH, HEADERS, MODULE_NAME, EXCEL_PATH
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS, XML_SETTINGS, \
    JSON_SETTINGS, FOLDER_SETTINGS, SYBASE_SETTINGS, ORACLE18c218c_SETTINGS
from ETLAuto.utils.commonutils import get_unique_id, upload_file
from ETLAuto.utils.paramizeutils import SettingsParam


class BaseHouse(object):
    def __init__(self,
                 mapping_from=None,
                 mapping_to=None,
                 reader_type=None,
                 db_type=None):
        self.reader_type = reader_type
        self.mapping_from = mapping_from
        self.mapping_to = mapping_to
        self.db_type = db_type
        self.uid = get_unique_id()
        self.settings = None
        self.reader_name = None
        self.writer_name = None
        self.collector_type = None

    @property
    def transformer(self):
        return NotImplemented

    def get_reader_info(self, mapping_from):
        if mapping_from == MYSQL_SETTINGS:
            reader_name = 'mysqlreader'
            db_type = 'mysql'
        elif mapping_from in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS]:
            reader_name = 'oraclereader'
            db_type = 'oracle'
        elif mapping_from == SQLSERVER_SETTINGS:
            reader_name = 'sqlserverreader'
            db_type = 'sqlserver'
        elif mapping_from == POSTGRESQL_SETTINGS:
            reader_name = 'postgresqlreader'
            db_type = 'postgresql'
        elif mapping_from == HIVE_SETTINGS:
            reader_name = 'hivereader'
            db_type = 'hive'
        elif mapping_from == SYBASE_SETTINGS:
            reader_name = 'sybasereader'
            db_type = 'sybase'
        else:
            raise ValueError('mapping_from error')
        return reader_name, db_type


#####################sql#########################
class BaseSQL(BaseHouse):
    def __init__(self,
                 mapping_from=None,
                 mapping_to=None,
                 reader_type=None,
                 src='11g',
                 dst='18c',
                 db_type=None,
                 table=None,
                 split_pk=None,
                 where=None,
                 lack=None,
                 view=None,
                 filter=None,
                 date=None,
                 replace=None,
                 merge=None,
                 custom=None,
                 dynamic=None,
                 static=None,
                 storage='parquet',
                 excel=None,
                 HAMode='false',
                 glob=False,
                 var=None,
                 nvarchar=None):
        super(BaseSQL, self).__init__(mapping_from=mapping_from,
                                      mapping_to=mapping_to,
                                      reader_type=reader_type,
                                      db_type=db_type)
        self.src = src
        self.dst = dst
        self.table = table
        self.split_pk = split_pk
        self.where = where
        self.lack = lack
        self.view = view
        self.filter = filter
        self.date = date
        self.replace = replace
        self.merge = merge
        self.custom = custom
        self.dynamic = dynamic
        self.static = static
        self.storage = storage
        self.excel = excel
        self.HAMode = HAMode
        self.glob = glob
        self.var = var
        self.nvarchar = nvarchar

    def get_table_columns(self):
        mapping_from = SettingsParam(self.mapping_from)
        _, db_type = self.get_reader_info(self.mapping_from)

        if self.mapping_from in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS] and self.table == 'normal':
            table_name = mapping_from.READER.table_normal
        else:
            table_name = mapping_from.READER.table

        if self.collector_type == 'increment':
            support_increment = True

            if self.mapping_from in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS]:
                table_name = os.environ.get('xtream_table', '')
        else:
            support_increment = False

        json_data = {'username': mapping_from.READER.user,
                     'password': mapping_from.READER.password,
                     'ip': mapping_from.READER.host,
                     'port': mapping_from.READER.port,
                     'dbName': mapping_from.READER.database,
                     'dbType': db_type,
                     'schema': mapping_from.READER.schema,
                     'tableName': table_name,
                     'supportIncrement': support_increment,
                     'haveKerberos': True,
                     }

        if self.mapping_from == HIVE_SETTINGS:
            del json_data['username']
            del json_data['password']

        if self.mapping_from in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS]:
            json_data.update({'serviceType': '服务名'})

        columns = API().get_table_columns(json_data)

        if self.lack:
            return self.alter_columns(columns, self.lack)
        return columns

    def alter_columns(self, columns, alter):
        if isinstance(alter, str):
            columns.remove(alter)
        elif isinstance(alter, list) or isinstance(alter, tuple):
            for item in alter:
                columns.remove(item)
        else:
            raise ValueError('type not support')
        return columns

    def upload_excel(self, json_data):
        host = os.environ[MODULE_NAME]
        url = host + URL.excel_upload
        headers = copy.copy(HEADERS)
        res = upload_file(url, headers, EXCEL_PATH, json_data)
        assert res.status_code == 200, res
        return res.json()

    @property
    def reader(self):
        json_data = {'username': self.settings.READER.user,
                     'password': self.settings.READER.password,
                     'ip': self.settings.READER.host,
                     'port': self.settings.READER.port,
                     'dbName': self.settings.READER.database,
                     'table': {self.settings.READER.schema: [self.settings.READER.table]},
                     'useSysChannelNumber': 'true',
                     'fetchSize': '1000',
                     'enableOffsetKey': 'false',
                     'dataSourceId': 'manual',
                     'column': self.get_table_columns(),
                     'dbType': self.db_type,
                     }

        if self.mapping_from in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS]:
            json_data.update({'serviceType': '服务名'})

            if self.split_pk:
                json_data.update({'splitPk': self.split_pk})

            if self.where:
                json_data.update({'where': self.where})

            if self.view:
                json_data.update({'table': {self.settings.READER.schema: \
                                                [self.settings.READER.view, self.settings.READER.table]}})

            if self.excel:
                form_data = {'param': json.dumps({'username': self.settings.READER.user,
                                                  'password': self.settings.READER.password,
                                                  'ip': self.settings.READER.host,
                                                  'port': self.settings.READER.port,
                                                  'dbName': self.settings.READER.database,
                                                  'serviceType': '服务名',
                                                  'dbType': self.db_type,
                                                  'excelType': os.path.splitext(EXCEL_PATH)[-1].strip('.'),
                                                  })
                             }
                excel_data = self.upload_excel(form_data).get('data', {})
                sheet_data = excel_data.get('data', {})
                schema = list(sheet_data.keys())[0]
                row_data_list = sheet_data.get(schema, {}).get('data', [])
                table = list(filter(lambda x: x.get('type') == 'T', row_data_list))[0].get('name')
                view = list(filter(lambda x: x.get('type') == 'V', row_data_list))[0].get('name')
                json_data.update({'table': {schema: [view, table]}})

            if self.mapping_to in [HDFS_SETTINGS, HBASE_SETTINGS, FOLDER_SETTINGS, \
                                   FILE_SETTINGS, SAMB_SETTINGS]:
                json_data.update({'table': {self.settings.READER.schema: \
                                                [self.settings.READER.table_normal]}})

        if self.mapping_from == HIVE_SETTINGS:
            json_data.update({'haveKerberos': 'true',
                              'useSysChannelNumber': 'false',
                              'channelNumber': '64',
                              'datasourceFrom': '平台内数据源',
                              })
            del json_data['username']
            del json_data['password']

        if self.collector_type == 'increment':
            if self.mapping_from in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS]:
                xtream_table = os.environ.get('xtream_table', '')
                json_data.update({'oracleVersion': self.settings.READER.version,
                                  'cdbName': self.settings.READER.database,
                                  'containerUsername': self.settings.READER.user,
                                  'containerPassword': self.settings.READER.password,
                                  'outServerName': self.settings.READER.xstream + xtream_table,
                                  'table': {self.settings.READER.schema: [xtream_table]},
                                  })
                reader_table = xtream_table
            else:
                reader_table = self.settings.READER.table

            json_data.update({'persistance': '否',
                              'primaryKey': {'{}_{}'.format(self.settings.READER.schema, reader_table): \
                                                 ['id']}})  # TODO: auto inspect primary key

        return {'parameter': json_data,
                'name': self.reader_name,
                }

    @property
    def transformer(self):
        if self.filter:
            json_data = [{'name': 'ds_myfilter',
                          'parameter': {'expression': self.filter},
                          }]
        elif self.date:
            json_data = [{'name': 'ds_dateConvert',
                          'parameter': {'columnName': self.date,
                                        'datatype': 'String',
                                        'layout_before': 'yyyy-MM-dd HH:mm:ss',
                                        'offset': '8',
                                        },
                          }]
        elif self.replace:
            json_data = [{'name': 'ds_string_replace',
                          'parameter': {'key': self.replace,
                                        'old': '男',
                                        'new': '女',
                                        },
                          }]
        elif self.merge:
            json_data = [{'name': 'ds_merge_columns',
                          'parameter': {'columns': ','.join(self.merge),
                                        'sep_char': '-',
                                        'new_column_name': 'res',
                                        'remove_old_column': 'true',
                                        },
                          }]
        elif self.custom:
            json_data = [{'name': 'ds_custom_column',
                          'parameter': {'column_name': self.custom,
                                        'column_type': 'string',
                                        'date_format': '',
                                        'custom_date_format': '',
                                        'value': 'aaa',
                                        },
                          }]
        else:
            json_data = []
        return json_data

    @property
    def writer(self):
        if self.reader_type == 'sql':
            if self.collector_type == 'increment':
                table_reader = os.environ.get('xtream_table', '')
            else:
                table_reader = self.mapping_from['READER']['table']

            table_map = {self.mapping_from['READER']['schema']: \
                             {table_reader: self.settings.WRITER.table + self.uid},
                         }

            if self.mapping_from in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS]:
                if self.view or self.excel:
                    table_map = {self.mapping_from['READER']['schema']: \
                                     {table_reader: self.settings.WRITER.table + self.uid,
                                      self.mapping_from['READER']['view']: self.settings.WRITER.view + self.uid,
                                      },
                                 }

            if self.var:
                table_map = {self.mapping_from['READER']['schema']: \
                                 {table_reader: self.settings.WRITER.table + self.var},
                             }

            json_data = {'username': self.settings.WRITER.user,
                         'password': self.settings.WRITER.password,
                         'ip': self.settings.WRITER.host,
                         'port': self.settings.WRITER.port,
                         'dbName': self.settings.WRITER.database,
                         'isTruncate': 'true',
                         'tablematchmode': '自定义',
                         'settablemaprelation': {},
                         'tabletoupper': '不变',
                         'columntoupper': '不变',
                         'storageformat': self.storage,
                         'presqlexectype': '无',
                         'postsqlexectype': '无',
                         'dataCheck': 'true',
                         'ftp': '无',
                         'dataSourceId': 'manual',
                         'tableautomatchrule': {'prefix': '',
                                                'prefixjoiner': '',
                                                'suffix': '',
                                                'suffixjoiner': '',
                                                'settablemaprelation': table_map,
                                                },
                         }
        elif self.reader_type == 'nosql':
            json_data = {'username': self.settings.WRITER.user,
                         'password': self.settings.WRITER.password,
                         'ip': self.settings.WRITER.host,
                         'port': self.settings.WRITER.port,
                         'dbName': self.settings.WRITER.database,
                         'isTruncate': 'true',
                         'tablematchmode': '手动',
                         'table': self.settings.WRITER.table,
                         'tabletoupper': '不变',
                         'columntoupper': '不变',
                         'storageformat': 'parquet',
                         'presqlexectype': '无',
                         'postsqlexectype': '无',
                         'dataSourceId': 'manual',
                         }
        else:
            raise ValueError('reader_type not support')

        if self.mapping_to in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS]:
            json_data.update({'serviceType': '服务名',
                              'writeschema': self.settings.WRITER.schema,
                              })

        if self.mapping_to == HIVE_SETTINGS:
            json_data.update({'haveKerberos': 'true',
                              'HAMode': self.HAMode,
                              'datasourceFrom': '平台内数据源',
                              })
            del json_data['username']
            del json_data['password']

            if self.mapping_from in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS]:
                if self.dynamic:
                    json_data.update({'partitionid': {self.mapping_from['READER']['table']: [self.dynamic]},
                                      'partitionType': '动态',
                                      })
                if self.static:
                    json_data.update({'partitionid': {self.mapping_from['READER']['table']: [self.static]},
                                      'partitionType': '静态',
                                      })

                if self.HAMode in ['true']:
                    payload = {'reader_name':'oraclereader',
                               'reader_format':None,
                               'reader_options':{'ip': self.mapping_from['READER']['host'],
                                                 'port': self.mapping_from['READER']['port'],
                                                 'username': self.mapping_from['READER']['user'],
                                                 'password': self.mapping_from['READER']['password'],
                                                 'serviceType': '服务名',
                                                 'dbName': self.mapping_from['READER']['database'],
                                                 'table': {self.mapping_from['READER']['schema']: \
                                                               [self.mapping_from['READER']['table']]},
                                                 'useSysChannelNumber': 'true',
                                                 'fetchSize': '1000',
                                                 'enableOffsetKey': 'false',
                                                 'dataSourceId': 'manual',
                                                 'column': self.get_table_columns(),
                                                 'dbType': 'oracle',
                                                 },
                               }
                    hive_writer_option = API().get_hive_writer_option(payload)
                    zookeeper_address = list(filter(lambda x: x.get('KeyName')=='zookeeperAddress', \
                                                    hive_writer_option))[0].get('Default')
                    zookeeper_namespace = list(filter(lambda x: x.get('KeyName')=='zookeeperNamespace', \
                                                      hive_writer_option))[0].get('Default')

                    json_data.update({'zookeeperAddress': zookeeper_address,
                                      'zookeeperNamespace': zookeeper_namespace,
                                      })

                if self.glob:
                    json_data.update({'presqlexectype': '全局执行',
                                      'postsqlexectype': '全局执行',
                                      'preSql': 'create table tmp_test(id int);insert into tmp_test values(1)',
                                      'postSql': 'drop table tmp_test',
                                      })


        if self.collector_type == 'increment':
            json_data.update({'partitionid': {}})
            del json_data['isTruncate']
            del json_data['tabletoupper']
            del json_data['columntoupper']
            del json_data['storageformat']
            del json_data['postsqlexectype']
            json_data['dataCheck'] = 'false'
            del json_data['ftp']
            del json_data['dataSourceId']

        return {'parameter': json_data,
                'name': self.writer_name,
                }


class Mysql(BaseSQL):
    def __init__(self, mapping_from=None, mapping_to=None, reader_type=None):
        super(Mysql, self).__init__(mapping_from=mapping_from, mapping_to=mapping_to, reader_type=reader_type)
        self.settings = SettingsParam(MYSQL_SETTINGS)
        self.reader_name = 'mysqlreader'
        self.writer_name = 'mysqlwriter'
        self.db_type = 'mysql'


class IncreaseMysql(BaseSQL):
    def __init__(self, mapping_from=None, mapping_to=None, reader_type=None):
        super(IncreaseMysql, self).__init__(mapping_from=mapping_from, mapping_to=mapping_to, reader_type=reader_type)
        self.settings = SettingsParam(MYSQL_SETTINGS)
        self.reader_name = 'cdcmysqlreader'
        self.collector_type = 'increment'
        self.db_type = 'cdcmysql'


class Oracle(BaseSQL):
    def __init__(self,
                 mapping_from=None,
                 mapping_to=None,
                 src='11g',
                 dst='18c',
                 reader_type=None,
                 table=None,
                 split_pk=None,
                 where=None,
                 lack=None,
                 view=None,
                 filter=None,
                 date=None,
                 replace=None,
                 merge=None,
                 custom=None,
                 excel=None,
                 nvarchar=None):
        super(Oracle, self).__init__(mapping_from=mapping_from,
                                     mapping_to=mapping_to,
                                     src=src,
                                     dst=dst,
                                     reader_type=reader_type,
                                     table=table,
                                     split_pk=split_pk,
                                     where=where,
                                     lack=lack,
                                     view=view,
                                     filter=filter,
                                     date=date,
                                     replace=replace,
                                     merge=merge,
                                     custom=custom,
                                     excel=excel,
                                     nvarchar=nvarchar)
        self.settings = SettingsParam(get_oracle_settings(self.src, self.dst))
        self.reader_name = 'oraclereader'
        self.writer_name = 'oraclewriter'
        self.db_type = 'oracle'


class IncreaseOracle(BaseSQL):
    def __init__(self, mapping_from=None, mapping_to=None, reader_type=None, src=None):
        super(IncreaseOracle, self).__init__(mapping_from=mapping_from,
                                             mapping_to=mapping_to,
                                             reader_type=reader_type,
                                             src=src)
        self.settings = SettingsParam(get_oracle_settings(self.src, self.dst))
        self.reader_name = 'cdcoraclereader'
        self.collector_type = 'increment'
        self.db_type = 'cdcoracle'


class Postgresql(BaseSQL):
    def __init__(self, mapping_from=None, mapping_to=None, reader_type=None):
        super(Postgresql, self).__init__(mapping_from=mapping_from, mapping_to=mapping_to, reader_type=reader_type)
        self.settings = SettingsParam(POSTGRESQL_SETTINGS)
        self.reader_name = 'postgresqlreader'
        self.writer_name = 'postgresqlwriter'
        self.db_type = 'postgresql'


class Sqlserver(BaseSQL):
    def __init__(self, mapping_from=None, mapping_to=None, reader_type=None):
        super(Sqlserver, self).__init__(mapping_from=mapping_from, mapping_to=mapping_to, reader_type=reader_type)
        self.settings = SettingsParam(SQLSERVER_SETTINGS)
        self.reader_name = 'sqlserverreader'
        self.writer_name = 'sqlserverwriter'
        self.db_type = 'sqlserver'


class Hive(BaseSQL):
    def __init__(self,
                 mapping_from=None,
                 mapping_to=None,
                 reader_type=None,
                 view=None,
                 filter=None,
                 dynamic=None,
                 static=None,
                 storage='parquet',
                 excel=None,
                 HAMode='false',
                 glob=False,
                 var=None):
        super(Hive, self).__init__(mapping_from=mapping_from,
                                   mapping_to=mapping_to,
                                   reader_type=reader_type,
                                   view=view,
                                   filter=filter,
                                   dynamic=dynamic,
                                   static=static,
                                   storage=storage,
                                   excel=excel,
                                   HAMode=HAMode,
                                   glob=glob,
                                   var=var)
        self.settings = SettingsParam(HIVE_SETTINGS)
        self.reader_name = 'hivereader'
        self.writer_name = 'hivewriter'
        self.db_type = 'hive'


class IncreaseHive(BaseSQL):
    def __init__(self, mapping_from=None, mapping_to=None, reader_type=None):
        super(IncreaseHive, self).__init__(mapping_from=mapping_from, mapping_to=mapping_to, reader_type=reader_type)
        self.settings = SettingsParam(HIVE_SETTINGS)
        self.writer_name = 'cdchivewriter'
        self.collector_type = 'increment'
        self.db_type = 'hive'


class Sybase(BaseSQL):
    def __init__(self, mapping_from=None, mapping_to=None, reader_type=None):
        super(Sybase, self).__init__(mapping_from=mapping_from, mapping_to=mapping_to, reader_type=reader_type)
        self.settings = SettingsParam(SYBASE_SETTINGS)
        self.reader_name = 'sybasereader'
        self.writer_name = 'sybasewriter'
        self.db_type = 'sybase'


#####################nosql#########################
class BaseNoSQL(BaseHouse):
    def __init__(self, mapping_from=None, reader_type=None, reader_format='csv', writer_format='csv'):
        super(BaseNoSQL, self).__init__(mapping_from=mapping_from, reader_type=reader_type)
        self.reader_format = reader_format
        self.writer_format = writer_format

    def get_file_path(self, seg=None):
        if seg == 'reader':
            if self.reader_type == 'orc':
                return self.settings.READER.path.orc
            return self.settings.READER.path.sql if self.reader_type == 'sql' else self.settings.READER.path.nosql
        elif seg == 'writer':
            return self.settings.WRITER.path.sql if self.reader_type == 'sql' else self.settings.WRITER.path.nosql
        else:
            raise ValueError('param not support')

    @property
    def transformer(self):
        return []

    # def get_format(self):
    #     if self.reader_type == 'sql':
    #         return 'csv'
    #     elif self.reader_type == 'nosql':
    #         return 'raw'
    #     else:
    #         raise ValueError('reader_type not support')


class HDFS(BaseNoSQL):
    def __init__(self, mapping_from=None, reader_type=None, reader_format='csv', writer_format='csv'):
        super(HDFS, self).__init__(mapping_from=mapping_from,
                                   reader_type=reader_type,
                                   reader_format=reader_format,
                                   writer_format=writer_format
                                   )
        self.settings = SettingsParam(HDFS_SETTINGS)
        self.reader_name = 'hdfsreader'
        self.writer_name = 'hdfswriter'

    @property
    def reader(self):
        if self.reader_format == 'TEXT':
            json_data = {'path': self.get_file_path(seg='reader'),
                         'format': self.reader_format,
                         'haveKerberos': 'true',
                         'ifWatch': 'false',
                         'useSysChannelNumber': 'true',
                         'dataSourceId': 'manual',
                         }
        elif self.reader_format == 'CSV':
            json_data = {'path': self.get_file_path(seg='reader'),
                         'format': self.reader_format,
                         'fieldDelimiter': ',',
                         'compress': 'none',
                         'haveKerberos': 'true',
                         'addColumns': 'false',
                         'useSysChannelNumber': 'true',
                         'dataSourceId': 'manual',
                         }
        elif self.reader_format == 'ORC':
            json_data = {'path': self.get_file_path(seg='reader'),
                         'format': self.reader_format,
                         'haveKerberos': 'true',
                         'useSysChannelNumber': 'true',
                         'dataSourceId': 'manual',
                         }
        else:
            raise ValueError('param not support')

        return {'parameter': json_data,
                'name': self.reader_name,
                }

    @property
    def writer(self):
        file_id = get_unique_id()
        filepath = self.get_file_path(seg='writer')
        path = os.path.dirname(filepath)
        filename = os.path.basename(filepath) + file_id

        if self.mapping_from == FOLDER_SETTINGS:
            format = 'binary'
        else:
            format = self.writer_format

        return {'parameter': {'path': path,
                              'fileName': filename,
                              'format': format,
                              'writeMode': 'truncate',
                              'fieldDelimiter': ',',
                              'haveKerberos': 'true',
                              'dataSourceId': 'manual',
                              },
                'name': self.writer_name,
                }


class Hbase(BaseNoSQL):
    def __init__(self, mapping_from=None, reader_type=None, reader_format='csv', writer_format='csv', column=None):
        super(Hbase, self).__init__(mapping_from=mapping_from,
                                    reader_type=reader_type,
                                    reader_format=reader_format,
                                    writer_format=writer_format
                                    )
        self.settings = SettingsParam(HBASE_SETTINGS)
        self.reader_name = 'hbase11xreader'
        self.writer_name = 'hbase11xwriter'
        self.column = column

    @property
    def reader(self):
        return

    def get_hbase_column_info(self):
        mapping_from = SettingsParam(self.mapping_from)
        reader_name, db_type = self.get_reader_info(self.mapping_from)

        json_data = {'username': mapping_from.READER.user,
                     'password': mapping_from.READER.password,
                     'ip': mapping_from.READER.host,
                     'port': mapping_from.READER.port,
                     'dbName': mapping_from.READER.database,
                     'readerName': reader_name,
                     'dbType': db_type,
                     'schema': mapping_from.READER.schema,
                     'tableName': mapping_from.READER.table,
                     'serviceType': '服务名',
                     }

        if self.mapping_from == ORACLE11g218c_SETTINGS and self.column == 'normal':
            json_data.update({'tableName': mapping_from.READER.table_normal})

        hbase_column_info = API().get_hbase_column_info(json_data)
        return hbase_column_info

    @property
    def writer(self):
        if self.reader_type == 'sql':
            json_data = {'table': self.settings.WRITER.table,
                         'encoding': self.settings.WRITER.charset,
                         'haveKerberos': 'true',
                         'dataSourceId': 'manual',
                         # 'rowkeySuffix': '',
                         'family': self.settings.WRITER.family,
                         'column': self.get_hbase_column_info(),
                         }
        elif self.reader_type == 'nosql':
            family = self.settings.WRITER.family

            # api not support dynamically get hbase column info
            names = ['id', 'name', 'address', 'phone_number', 'email', 'ip']
            types = ['int', 'string', 'string', 'string', 'string', 'string']
            column = ';'.join(['{}:{},{}'.format(family, n, t) for n, t in zip(names, types)])

            json_data = {'table': self.settings.WRITER.table,
                         'encoding': self.settings.WRITER.charset,
                         'haveKerberos': 'true',
                         'dataSourceId': 'manual',
                         # 'rowkeySuffix': '',
                         'column': column,
                         }
        else:
            raise ValueError('reader_type not support')

        return {'parameter': json_data,
                'name': self.writer_name,
                }


class Ftp(BaseNoSQL):
    def __init__(self, mapping_from=None, reader_type=None, reader_format='csv', writer_format='csv'):
        super(Ftp, self).__init__(mapping_from=mapping_from,
                                  reader_type=reader_type,
                                  reader_format=reader_format,
                                  writer_format=writer_format
                                  )
        self.settings = SettingsParam(SAMB_SETTINGS)
        self.reader_name = 'ftpreader'
        self.writer_name = 'ftpwriter'

    @property
    def reader(self):
        filepath = self.get_file_path(seg='reader')
        path = os.path.dirname(filepath)
        filename = os.path.basename(filepath)

        return {'parameter': {'host': self.settings.READER.host,
                              'protocol': 'ftp',
                              'port': self.settings.READER.port,
                              'anonymous': 'false',
                              'username': self.settings.READER.user,
                              'password': self.settings.READER.password,
                              'path': path,
                              'fileWildcard': filename,
                              'writeMode': 'truncate',
                              'format': self.reader_format,
                              'fieldDelimiter': ',',
                              'connectPattern': 'PORT',
                              'compress': 'none',
                              'encoding': self.settings.READER.charset,
                              'addColumns': 'false',
                              'useSysChannelNumber': 'true',
                              'dataSourceId': 'manual',
                              },
                'name': self.reader_name,
                }

    @property
    def writer(self):
        filepath = self.get_file_path(seg='writer')
        path = os.path.dirname(filepath)
        filename = os.path.basename(filepath)

        return {'parameter': {'host': self.settings.WRITER.host,
                              'protocol': 'ftp',
                              'port': self.settings.WRITER.port,
                              'anonymous': 'false',
                              'username': self.settings.WRITER.user,
                              'password': self.settings.WRITER.password,
                              'path': path,
                              'fileName': filename,
                              'writeMode': 'truncate',
                              'format': self.writer_format,
                              'fieldDelimiter': ',',
                              'dataSourceId': 'manual',
                              },
                'name': self.writer_name,
                }


class File(BaseNoSQL):
    def __init__(self, mapping_from=None, reader_type=None, reader_format='csv', writer_format='csv'):
        super(File, self).__init__(mapping_from=mapping_from,
                                   reader_type=reader_type,
                                   reader_format=reader_format,
                                   writer_format=writer_format
                                   )
        self.settings = SettingsParam(FILE_SETTINGS)
        self.reader_name = 'txtfilereader'
        self.writer_name = 'txtfilewriter'

    @property
    def reader(self):
        # ip_list = API().get_agent_list()
        return {'parameter': {'ip': self.settings.READER.host,
                              'path': self.get_file_path(seg='reader'),
                              'encoding': self.settings.READER.charset,
                              'format': self.reader_format,
                              'fieldDelimiter': ',',
                              'ifWatch': 'false',
                              'addColumns': 'false',
                              'useSysChannelNumber': 'true',
                              },
                'name': self.reader_name,
                }

    @property
    def writer(self):
        filepath = self.get_file_path(seg='writer')
        path = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        ip_list = API().get_agent_list()

        return {'parameter': {'ip': random.choice(ip_list),
                              'path': path,
                              'fileName': filename,
                              'writeMode': 'truncate',
                              'format': self.writer_format,
                              'fieldDelimiter': ',',
                              'encoding': self.settings.WRITER.charset,
                              },
                'name': self.writer_name,
                }


class Xml(BaseNoSQL):
    def __init__(self, mapping_from=None, reader_type=None, reader_format=None, writer_format=None):
        super(Xml, self).__init__(mapping_from=mapping_from,
                                  reader_type=reader_type,
                                  reader_format=reader_format,
                                  writer_format=writer_format
                                  )
        self.settings = SettingsParam(XML_SETTINGS)
        self.reader_name = 'xmlorjsonreader'
        self.writer_name = ''

    @property
    def reader(self):
        return {'parameter': {'ip': self.settings.READER.host,
                              'filepath': self.get_file_path(seg='reader'),
                              'encoding': self.settings.READER.charset,
                              'format': self.settings.READER.format,
                              'xpath': '/root',
                              'ifatributeaselement': 'true',
                              'useSysChannelNumber': 'true',
                              },
                'name': self.reader_name,
                }


class Json(BaseNoSQL):
    def __init__(self, mapping_from=None, reader_type=None, reader_format=None, writer_format=None):
        super(Json, self).__init__(mapping_from=mapping_from,
                                   reader_type=reader_type,
                                   reader_format=reader_format,
                                   writer_format=writer_format
                                   )
        self.settings = SettingsParam(JSON_SETTINGS)
        self.reader_name = 'xmlorjsonreader'
        self.writer_name = ''

    @property
    def reader(self):
        return {'parameter': {'ip': self.settings.READER.host,
                              'filepath': self.get_file_path(seg='reader'),
                              'encoding': self.settings.READER.charset,
                              'format': self.settings.READER.format,
                              'useSysChannelNumber': 'true',
                              },
                'name': self.reader_name,
                }


class Folder(BaseNoSQL):
    def __init__(self, mapping_from=None, reader_type=None, reader_format=None, writer_format=None):
        super(Folder, self).__init__(mapping_from=mapping_from,
                                     reader_type=reader_type,
                                     reader_format=reader_format,
                                     writer_format=writer_format
                                     )
        self.settings = SettingsParam(FOLDER_SETTINGS)
        self.reader_name = 'folderreader'
        self.writer_name = ''

    @property
    def reader(self):
        return {'parameter': {'ip': self.settings.READER.host,
                              'path': self.get_file_path(seg='reader'),
                              'encoding': self.settings.READER.charset,
                              'format': self.settings.READER.format,
                              'useSysChannelNumber': 'true',
                              },
                'name': self.reader_name,
                }


class Kafka(BaseNoSQL):
    def __init__(self, mapping_from=None, reader_type=None, reader_format='csv', writer_format='csv'):
        super(Kafka, self).__init__(mapping_from=mapping_from,
                                    reader_type=reader_type,
                                    reader_format=reader_format,
                                    writer_format=writer_format
                                    )
        self.settings = SettingsParam(KAFKA_SETTINGS)
        self.reader_name = 'streamkafkareader'
        self.writer_name = 'streamkafkawriter'

    @property
    def reader(self):
        group_id = get_unique_id()
        return {'parameter': {'servers': self.settings.READER.servers,
                              'topic': self.settings.READER.topic,
                              # 'format': self.settings.READER.format,
                              'groupId': group_id,
                              'haveKerberos': 'true',
                              'principal': self.settings.READER.principal,
                              'keytab': self.keytab_path,
                              'useSysChannelNumber': 'true',
                              'protocol': self.settings.READER.protocol,
                              'persistance': '否',
                              'dataSourceId': 'manual',
                              },
                'name': self.reader_name,
                }

    @property
    def writer(self):
        return {'parameter': {'servers': self.settings.WRITER.servers,
                              'topic': self.settings.WRITER.topic,
                              'format': self.writer_format,
                              'haveKerberos': 'true',
                              'principal': self.settings.WRITER.principal,
                              'keytab': self.keytab_path,
                              'protocol': self.settings.WRITER.protocol,
                              'dataSourceId': 'manual',
                              },
                'name': self.writer_name,
                }

    @property
    def keytab_path(self):
        host = os.environ[MODULE_NAME]
        url = host + URL.file_upload
        headers = copy.copy(HEADERS)
        res = upload_file(url, headers, KEYTAB_PATH)
        assert res.status_code == 200, res
        return res.content.decode()

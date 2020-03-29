#!/usr/bin/python
# -*- coding: utf-8 -*-


import json

from ETLAuto.settings import ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS, HIVE_SETTINGS
from ETLAuto.utils.commonutils import get_unique_id
from ETLAuto.utils.paramizeutils import SettingsParam


class BaseDB(object):
    def __init__(self, settings=None, type=None):
        self.settings = settings
        self.type = type

    @property
    def params(self):
        return {'name': '_'.join([self.type, get_unique_id()]),
                'type': self.type,
                'config': json.dumps(self.config),
                'note': '',
                }

    @property
    def config(self):
        return NotImplemented


class BaseSQL(BaseDB):
    def __init__(self, settings=None, type=None, dbtype=None, support_increment=None):
        super(BaseSQL, self).__init__(settings=settings, type=type)
        self.dbtype = dbtype
        self.support_increment = support_increment

    @property
    def config(self):
        json_data = {'username': SettingsParam(self.settings).READER.user,
                     'password': SettingsParam(self.settings).READER.password,
                     'ip': SettingsParam(self.settings).READER.host,
                     'port': SettingsParam(self.settings).READER.port,
                     'dbName': SettingsParam(self.settings).READER.database,
                     }

        if self.dbtype:
            json_data.update({'dbType': self.dbtype})

        if self.support_increment is not None:
            json_data.update({'supportIncrement': self.support_increment})

        if self.settings in [ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS]:
            json_data.update({'serviceType': u'服务名'})

        if self.settings == HIVE_SETTINGS:
            json_data.update({'haveKerberos': 'true',
                              'datasourceFrom': '平台内数据源',
                              })

        return json_data


class Mysql(BaseSQL):
    def __init__(self, settings=None, dbtype='mysql', support_increment=False):
        super(Mysql, self).__init__(settings=settings, dbtype=dbtype, support_increment=support_increment)


class Oracle(BaseSQL):
    def __init__(self, settings=None, dbtype='oracle', support_increment=False):
        super(Oracle, self).__init__(settings=settings, dbtype=dbtype, support_increment=support_increment)


class Postgresql(BaseSQL):
    def __init__(self, settings=None, dbtype='postgresql', support_increment=False):
        super(Postgresql, self).__init__(settings=settings, dbtype=dbtype, support_increment=support_increment)


class Sqlserver(BaseSQL):
    def __init__(self, settings=None, dbtype='sqlserver', support_increment=False):
        super(Sqlserver, self).__init__(settings=settings, dbtype=dbtype, support_increment=support_increment)


class Hive(BaseSQL):
    def __init__(self, settings=None, dbtype='hive', support_increment=False):
        super(Hive, self).__init__(settings=settings, dbtype=dbtype, support_increment=support_increment)


class Sybase(BaseSQL):
    def __init__(self, settings=None, dbtype='sybase', support_increment=False):
        super(Sybase, self).__init__(settings=settings, dbtype=dbtype, support_increment=support_increment)


class BaseNosql(BaseDB):
    def __init__(self, settings=None, type=None):
        super(BaseNosql, self).__init__(settings=settings, type=type)

    @property
    def config(self):
        return NotImplemented


class Hbase(BaseNosql):
    def __init__(self, settings=None, type=None):
        super(Hbase, self).__init__(settings=settings, type=type)

    @property
    def config(self):
        return {'table': SettingsParam(self.settings).WRITER.table,
                'encoding': SettingsParam(self.settings).WRITER.charset,
                'haveKerberos': 'true',
                }


class HDFS(BaseNosql):
    def __init__(self, settings=None, type=None):
        super(HDFS, self).__init__(settings=settings, type=type)

    @property
    def config(self):
        return {'path': SettingsParam(self.settings).READER.path.sql,
                'haveKerberos': 'true',
                }


class Ftp(BaseNosql):
    def __init__(self, settings=None, type=None):
        super(Ftp, self).__init__(settings=settings, type=type)

    @property
    def config(self):
        return {'host': SettingsParam(self.settings).READER.host,
                'protocol': 'ftp',
                'port': SettingsParam(self.settings).READER.port,
                'anonymous': 'false',
                'username': SettingsParam(self.settings).READER.user,
                'password': SettingsParam(self.settings).READER.password,
                }

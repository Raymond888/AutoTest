#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka, IncreaseMysql, IncreaseHive, Sybase
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS, SYBASE_SETTINGS


#################MysqlConfigs##################
class Sybase2Mysql(RunnerConfig):
    def __init__(self):
        super(Sybase2Mysql, self).__init__()

    @property
    def wh_reader(self):
        return Sybase(mapping_from=SYBASE_SETTINGS)

    @property
    def wh_transformer(self):
        return Sybase()

    @property
    def wh_writer(self):
        return Mysql(mapping_from=SYBASE_SETTINGS, reader_type=WARETYPE[0])


class Sybase2Oracle(Sybase2Mysql):
    @property
    def wh_writer(self):
        return Oracle(mapping_from=SYBASE_SETTINGS, mapping_to=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[0])


class Sybase2Hive(Sybase2Mysql):
    @property
    def wh_writer(self):
        return Hive(mapping_from=SYBASE_SETTINGS, mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[0])


class Sybase2HDFS(Sybase2Mysql):
    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='csv')


class Sybase2Postgresql(Sybase2Mysql):
    @property
    def wh_writer(self):
        return Postgresql(mapping_from=SYBASE_SETTINGS, reader_type=WARETYPE[0])


class Sybase2Sqlserver(Sybase2Mysql):
    @property
    def wh_writer(self):
        return Sqlserver(mapping_from=SYBASE_SETTINGS, reader_type=WARETYPE[0])

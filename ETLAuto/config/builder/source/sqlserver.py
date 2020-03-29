#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS


#################MysqlConfigs##################
class Sqlserver2Sqlserver(RunnerConfig):
    @property
    def wh_reader(self):
        return Sqlserver(mapping_from=SQLSERVER_SETTINGS)

    @property
    def wh_transformer(self):
        return Sqlserver()

    @property
    def wh_writer(self):
        return Sqlserver(mapping_from=SQLSERVER_SETTINGS, reader_type=WARETYPE[0])


class Sqlserver2Mysql(Sqlserver2Sqlserver):
    @property
    def wh_writer(self):
        return Mysql(mapping_from=SQLSERVER_SETTINGS, reader_type=WARETYPE[0])


class Sqlserver2Oracle(Sqlserver2Sqlserver):
    @property
    def wh_writer(self):
        return Oracle(mapping_from=SQLSERVER_SETTINGS, mapping_to=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[0])


class Sqlserver2Hive(Sqlserver2Sqlserver):
    @property
    def wh_writer(self):
        return Hive(mapping_from=SQLSERVER_SETTINGS, mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[0])


class Sqlserver2HDFS(Sqlserver2Sqlserver):
    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='csv')


class Sqlserver2Postgresql(Sqlserver2Sqlserver):
    @property
    def wh_writer(self):
        return Postgresql(mapping_from=SQLSERVER_SETTINGS, reader_type=WARETYPE[0])


class Sqlserver2Hbase(Sqlserver2Sqlserver):
    @property
    def wh_writer(self):
        return Hbase(mapping_from=SQLSERVER_SETTINGS, reader_type=WARETYPE[0])


class Sqlserver2Ftp(Sqlserver2Sqlserver):
    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[0], writer_format='csv')


class Sqlserver2File(Sqlserver2Sqlserver):
    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='csv')


class Sqlserver2Kafka(Sqlserver2Sqlserver):
    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[0], writer_format='csv')

#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS


#################MysqlConfigs##################
class Postgresql2Postgresql(RunnerConfig):
    @property
    def wh_reader(self):
        return Postgresql(mapping_from=POSTGRESQL_SETTINGS)

    @property
    def wh_transformer(self):
        return Postgresql()

    @property
    def wh_writer(self):
        return Postgresql(mapping_from=POSTGRESQL_SETTINGS, mapping_to=POSTGRESQL_SETTINGS, reader_type=WARETYPE[0])


class Postgresql2Mysql(Postgresql2Postgresql):
    @property
    def wh_writer(self):
        return Mysql(mapping_from=POSTGRESQL_SETTINGS, mapping_to=MYSQL_SETTINGS, reader_type=WARETYPE[0])


class Postgresql2Oracle(Postgresql2Postgresql):
    @property
    def wh_writer(self):
        return Oracle(mapping_from=POSTGRESQL_SETTINGS, mapping_to=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[0])


class Postgresql2Hive(Postgresql2Postgresql):
    @property
    def wh_writer(self):
        return Hive(mapping_from=POSTGRESQL_SETTINGS, mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[0])


class Postgresql2HDFS(Postgresql2Postgresql):
    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='csv')


class Postgresql2Hbase(Postgresql2Postgresql):
    @property
    def wh_writer(self):
        return Hbase(mapping_from=POSTGRESQL_SETTINGS, reader_type=WARETYPE[0])


class Postgresql2Sqlserver(Postgresql2Postgresql):
    @property
    def wh_writer(self):
        return Sqlserver(mapping_from=POSTGRESQL_SETTINGS, mapping_to=SQLSERVER_SETTINGS, reader_type=WARETYPE[0])


class Postgresql2Ftp(Postgresql2Postgresql):
    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[0], writer_format='csv')


class Postgresql2File(Postgresql2Postgresql):
    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='csv')


class Postgresql2Kafka(Postgresql2Postgresql):
    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[0], writer_format='csv')

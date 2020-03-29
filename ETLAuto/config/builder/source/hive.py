#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS


#################MysqlConfigs##################
class Hive2Hive(RunnerConfig):
    @property
    def wh_reader(self):
        return Hive(mapping_from=HIVE_SETTINGS)

    @property
    def wh_transformer(self):
        return Hive()

    @property
    def wh_writer(self):
        return Hive(mapping_from=HIVE_SETTINGS, mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[0])


class Hive2Mysql(Hive2Hive):
    @property
    def wh_writer(self):
        return Mysql(mapping_from=HIVE_SETTINGS, reader_type=WARETYPE[0])


class Hive2Oracle(Hive2Hive):
    @property
    def wh_writer(self):
        return Oracle(mapping_from=HIVE_SETTINGS, mapping_to=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[0])


class Hive2HDFS(Hive2Hive):
    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='csv')


class Hive2Postgresql(Hive2Hive):
    @property
    def wh_writer(self):
        return Postgresql(mapping_from=HIVE_SETTINGS, reader_type=WARETYPE[0])


class Hive2Hbase(Hive2Hive):
    @property
    def wh_writer(self):
        return Hbase(mapping_from=HIVE_SETTINGS, reader_type=WARETYPE[0])


class Hive2Sqlserver(Hive2Hive):
    @property
    def wh_writer(self):
        return Sqlserver(mapping_from=HIVE_SETTINGS, reader_type=WARETYPE[0])


class Hive2Ftp(Hive2Hive):
    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[0], writer_format='csv')


class Hive2File(Hive2Hive):
    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='csv')


class Hive2Kafka(Hive2Hive):
    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[0], writer_format='csv')

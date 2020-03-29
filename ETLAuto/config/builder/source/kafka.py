#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS


#################FileConfigs##################
class Kafka2Kafka(RunnerConfig):
    @property
    def wh_reader(self):
        return Kafka()

    @property
    def wh_transformer(self):
        return Kafka()

    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[1])


class Kafka2File(Kafka2Kafka):
    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[1], writer_format='raw')


class Kafka2HDFS(Kafka2Kafka):
    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[1], writer_format='raw')


class Kafka2Sqlserver(Kafka2Kafka):
    @property
    def wh_writer(self):
        return Sqlserver(reader_type=WARETYPE[1])


class Kafka2Ftp(Kafka2Kafka):
    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[1], writer_format='raw')

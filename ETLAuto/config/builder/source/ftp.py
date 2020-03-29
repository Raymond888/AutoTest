#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS


#################FtpConfigs##################
class Ftp2Ftp(RunnerConfig):
    @property
    def wh_reader(self):
        return Ftp(reader_type=WARETYPE[0], reader_format='csv')

    @property
    def wh_transformer(self):
        return Ftp()

    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[0], writer_format='csv')


class Ftp2HDFS(Ftp2Ftp):
    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='csv')


class Ftp2Kafka(Ftp2Ftp):
    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[0], writer_format='csv')
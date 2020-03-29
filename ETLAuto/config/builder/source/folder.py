#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka, Folder
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS, FOLDER_SETTINGS


#################FtpConfigs##################
class Folder2Folder(RunnerConfig):
    @property
    def wh_reader(self):
        return Folder(reader_type=WARETYPE[0])

    @property
    def wh_transformer(self):
        return Folder()

    @property
    def wh_writer(self):
        return Folder(reader_type=WARETYPE[0])


class Folder2HDFS(Folder2Folder):
    @property
    def wh_writer(self):
        return HDFS(mapping_from=FOLDER_SETTINGS, reader_type=WARETYPE[0])
#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS


#################FtpConfigs##################
class HDFS2HDFSCsv2Csv(RunnerConfig):
    @property
    def wh_reader(self):
        return HDFS(reader_type=WARETYPE[0], reader_format='CSV')

    @property
    def wh_transformer(self):
        return HDFS()

    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='CSV')


class HDFS2HDFSText2Text(RunnerConfig):
    @property
    def wh_reader(self):
        return HDFS(reader_type=WARETYPE[0], reader_format='TEXT')

    @property
    def wh_transformer(self):
        return HDFS()

    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='TEXT')


class HDFS2HDFSOrc2Orc(RunnerConfig):
    @property
    def wh_reader(self):
        return HDFS(reader_type=WARETYPE[2], reader_format='ORC')

    @property
    def wh_transformer(self):
        return HDFS()

    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='ORC')


class HDFS2FileCsv2Csv(HDFS2HDFSCsv2Csv):
    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='csv')


class HDFS2FileText2Text(HDFS2HDFSText2Text):
    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='raw')


class HDFS2FileOrc2Orc(HDFS2HDFSOrc2Orc):
    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='csv')


class HDFS2Ftp(HDFS2HDFSCsv2Csv):
    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[0])


class HDFS2Kafka(HDFS2HDFSCsv2Csv):
    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[0], writer_format='csv')


class HDFS2Oracle(HDFS2HDFSCsv2Csv):
    @property
    def wh_writer(self):
        return Oracle(mapping_to=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[1])


class HDFS2Postgresql(HDFS2HDFSCsv2Csv):
    @property
    def wh_writer(self):
        return Postgresql(reader_type=WARETYPE[1])


class HDFS2Sqlserver(HDFS2HDFSCsv2Csv):
    @property
    def wh_writer(self):
        return Sqlserver(reader_type=WARETYPE[1])


class HDFS2Mysql(HDFS2HDFSCsv2Csv):
    @property
    def wh_writer(self):
        return Mysql(reader_type=WARETYPE[1])
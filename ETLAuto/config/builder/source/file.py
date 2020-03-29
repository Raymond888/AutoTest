#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS


#################FileConfigs##################
class File2FileCsv2Csv(RunnerConfig):
    @property
    def wh_reader(self):
        return File(reader_type=WARETYPE[0], reader_format='csv')

    @property
    def wh_transformer(self):
        return File()

    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='csv')


class File2FileRaw2Raw(RunnerConfig):
    @property
    def wh_reader(self):
        return File(reader_type=WARETYPE[0], reader_format='raw')

    @property
    def wh_transformer(self):
        return File()

    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='raw')


class File2HDFSCsv2Csv(File2FileCsv2Csv):
    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='csv')


class File2HDFSRaw2Raw(File2FileRaw2Raw):
    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='raw')


class File2KafkaCsv2Csv(File2FileCsv2Csv):
    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[0], writer_format='csv')


class File2KafkaRaw2Raw(File2FileRaw2Raw):
    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[0], writer_format='raw')


class File2Sqlserver(File2FileCsv2Csv):
    @property
    def wh_writer(self):
        return Sqlserver(reader_type=WARETYPE[1])


class File2FtpCsv2Csv(File2FileCsv2Csv):
    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[0], writer_format='csv')


class File2FtpRaw2Raw(File2FileRaw2Raw):
    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[0], writer_format='raw')


class File2Mysql(File2FileCsv2Csv):
    @property
    def wh_writer(self):
        return Mysql(reader_type=WARETYPE[1])


class File2Oracle(File2FileCsv2Csv):
    @property
    def wh_writer(self):
        return Oracle(mapping_to=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[1])


class File2Hive(File2FileCsv2Csv):
    @property
    def wh_writer(self):
        return Hive(mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[1])


class File2Hbase(File2FileCsv2Csv):
    @property
    def wh_writer(self):
        return Hbase(reader_type=WARETYPE[1])

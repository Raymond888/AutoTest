#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka, IncreaseMysql, IncreaseHive
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS


#################MysqlConfigs##################
class Mysql2Mysql(RunnerConfig):
    def __init__(self):
        super(Mysql2Mysql, self).__init__()

    @property
    def wh_reader(self):
        return Mysql(mapping_from=MYSQL_SETTINGS)

    @property
    def wh_transformer(self):
        return Mysql()

    @property
    def wh_writer(self):
        return Mysql(mapping_from=MYSQL_SETTINGS, reader_type=WARETYPE[0])


class Mysql2Oracle(Mysql2Mysql):
    @property
    def wh_writer(self):
        return Oracle(mapping_from=MYSQL_SETTINGS, mapping_to=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[0])


class Mysql2Hive(Mysql2Mysql):
    @property
    def wh_writer(self):
        return Hive(mapping_from=MYSQL_SETTINGS, mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[0])


class IncreaseMysql2Hive(Mysql2Mysql):
    def __init__(self):
        super(IncreaseMysql2Hive, self).__init__()
        self.collector_type = 'increment'

    @property
    def wh_reader(self):
        return IncreaseMysql(mapping_from=MYSQL_SETTINGS)

    @property
    def wh_writer(self):
        return IncreaseHive(mapping_from=MYSQL_SETTINGS, mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[0])


class Mysql2HDFS(Mysql2Mysql):
    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='csv')


class Mysql2Postgresql(Mysql2Mysql):
    @property
    def wh_writer(self):
        return Postgresql(mapping_from=MYSQL_SETTINGS, reader_type=WARETYPE[0])


class Mysql2Hbase(Mysql2Mysql):
    @property
    def wh_writer(self):
        return Hbase(mapping_from=MYSQL_SETTINGS, reader_type=WARETYPE[0])


class Mysql2Sqlserver(Mysql2Mysql):
    @property
    def wh_writer(self):
        return Sqlserver(mapping_from=MYSQL_SETTINGS, reader_type=WARETYPE[0])


class Mysql2Ftp(Mysql2Mysql):
    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[0], writer_format='csv')


class Mysql2File(Mysql2Mysql):
    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='csv')


class Mysql2Kafka(Mysql2Mysql):
    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[0], writer_format='csv')

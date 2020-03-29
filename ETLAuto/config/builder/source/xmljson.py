#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka, Xml, Json
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS


#################FileConfigs##################
class Xml2Xml(RunnerConfig):
    @property
    def wh_reader(self):
        return Xml(reader_type=WARETYPE[0])

    @property
    def wh_transformer(self):
        return Xml()

    @property
    def wh_writer(self):
        return Xml(reader_type=WARETYPE[0])


class Xml2Mysql(Xml2Xml):
    @property
    def wh_writer(self):
        return Mysql(reader_type=WARETYPE[1])


class Xml2Oracle(Xml2Xml):
    @property
    def wh_writer(self):
        return Oracle(mapping_to=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[1])


class Xml2Hive(Xml2Xml):
    @property
    def wh_writer(self):
        return Hive(mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[1])


class Json2Json(RunnerConfig):
    @property
    def wh_reader(self):
        return Json(reader_type=WARETYPE[0])

    @property
    def wh_transformer(self):
        return Json()

    @property
    def wh_writer(self):
        return Json(reader_type=WARETYPE[0])


class Json2Mysql(Json2Json):
    @property
    def wh_writer(self):
        return Mysql(reader_type=WARETYPE[1])


class Json2Oracle(Json2Json):
    @property
    def wh_writer(self):
        return Oracle(mapping_to=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[1])


class Json2Hive(Json2Json):
    @property
    def wh_writer(self):
        return Hive(mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[1])

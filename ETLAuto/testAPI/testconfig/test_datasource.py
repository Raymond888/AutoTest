#!/usr/bin/python
# -*- coding: utf-8 -*-


import pytest

from ETLAuto.testAPI.testconfig.base import TestConfigManager
from ETLAuto.testAPI.testconfig.dbconfig import BaseSQL, Hbase, HDFS, Ftp
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS


@pytest.mark.run(order=3)
class TestDatasource(TestConfigManager):
    def test_add_mysql(self):
        json_data = BaseSQL(settings=MYSQL_SETTINGS, type='mysqlreader').params
        self.add_data(json_data)

    def test_add_oracle(self):
        json_data = BaseSQL(settings=ORACLE11g218c_SETTINGS, type='oraclereader').params
        self.add_data(json_data)

    def test_add_postgresql(self):
        json_data = BaseSQL(settings=POSTGRESQL_SETTINGS, type='postgresqlreader').params
        self.add_data(json_data)

    def test_add_sqlserver(self):
        json_data = BaseSQL(settings=SQLSERVER_SETTINGS, type='sqlserverreader').params
        self.add_data(json_data)

    def test_add_hive(self):
        json_data = BaseSQL(settings=HIVE_SETTINGS, type='hivereader').params
        self.add_data(json_data)

    def test_add_Hbase(self):
        json_data = Hbase(settings=HBASE_SETTINGS, type='hbase11xreader').params
        self.add_data(json_data)

    def test_add_HDFS(self):
        json_data = HDFS(settings=HDFS_SETTINGS, type='hdfsreader').params
        self.add_data(json_data)

    def test_add_Ftp(self):
        json_data = Ftp(settings=SAMB_SETTINGS, type='ftpreader').params
        self.add_data(json_data)


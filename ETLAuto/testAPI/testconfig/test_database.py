#!/usr/bin/python
# -*- coding: utf-8 -*-


import pytest

from ETLAuto.objects.base import BaseDatabase
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS, POSTGRESQL_SETTINGS, \
    SQLSERVER_SETTINGS, HIVE_SETTINGS, SYBASE_SETTINGS
from ETLAuto.testAPI.testconfig.dbconfig import Mysql, Oracle, Postgresql, Sqlserver, Hive, Sybase


class TestDatabaseManager(object):
    def check_database(self, json_data, settings):
        db = BaseDatabase()
        res_data = db.all_table(json_data)

        database = settings['READER']['schema']
        table = settings['READER']['table']

        assert database in res_data
        assert table in res_data.get(database, [])


@pytest.mark.run(order=1)
class TestDatabase(TestDatabaseManager):
    def test_mysql(self):
        json_data = Mysql(settings=MYSQL_SETTINGS).config
        self.check_database(json_data, MYSQL_SETTINGS)

    def test_oracle_11g(self):
        json_data = Oracle(settings=ORACLE11g218c_SETTINGS).config
        self.check_database(json_data, ORACLE11g218c_SETTINGS)

    def test_oracle_18c(self):
        json_data = Oracle(settings=ORACLE18c218c_SETTINGS).config
        self.check_database(json_data, ORACLE18c218c_SETTINGS)

    def test_oracle_11g_increase(self):
        json_data = Oracle(settings=ORACLE11g218c_SETTINGS, support_increment=True).config
        self.check_database(json_data, ORACLE11g218c_SETTINGS)

    def test_oracle_18c_increase(self):
        json_data = Oracle(settings=ORACLE18c218c_SETTINGS, support_increment=True).config
        self.check_database(json_data, ORACLE18c218c_SETTINGS)

    def test_postgresql(self):
        json_data = Postgresql(settings=POSTGRESQL_SETTINGS).config
        self.check_database(json_data, POSTGRESQL_SETTINGS)

    def test_sqlserver(self):
        json_data = Sqlserver(settings=SQLSERVER_SETTINGS).config
        self.check_database(json_data, SQLSERVER_SETTINGS)

    def test_hive(self):
        json_data = Hive(settings=HIVE_SETTINGS).config
        self.check_database(json_data, HIVE_SETTINGS)

    def test_sybase(self):
        json_data = Sybase(settings=SYBASE_SETTINGS).config
        self.check_database(json_data, SYBASE_SETTINGS)

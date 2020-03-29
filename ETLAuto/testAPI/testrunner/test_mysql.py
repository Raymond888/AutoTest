#!/usr/bin/python
# -*- coding: utf-8 -*-

import pytest

from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.mysql import Mysql2MysqlRunner, Mysql2OracleRunner, Mysql2HiveRunner, \
    Mysql2HDFSRunner, Mysql2PostgresqlRunner, Mysql2HbaseRunner, Mysql2SqlserverRunner, Mysql2FtpRunner, \
    Mysql2FileRunner, Mysql2KafkaRunner, IncreaseMysql2HiveRunner


@pytest.mark.run(order=2)
class TestRunnerMysql(TestRunnerManager):
    def test_runner_mysql2hive_increase(self):
        self.start_runner(IncreaseMysql2HiveRunner)

    def test_runner_mysql2mysql(self):
        self.start_runner(Mysql2MysqlRunner)

    def test_runner_mysql2oracle(self):
        self.start_runner(Mysql2OracleRunner)

    def test_runner_mysql2hive(self):
        self.start_runner(Mysql2HiveRunner)

    def test_runner_mysql2HDFS(self):
        self.start_runner(Mysql2HDFSRunner)

    def test_runner_mysql2postgresql(self):
        self.start_runner(Mysql2PostgresqlRunner)

    def test_runner_mysql2Hbase(self):
        self.start_runner(Mysql2HbaseRunner)

    def test_runner_mysql2sqlserver(self):
        self.start_runner(Mysql2SqlserverRunner)

    def test_runner_mysql2ftp(self):
        self.start_runner(Mysql2FtpRunner)

    def test_runner_mysql2file(self):
        self.start_runner(Mysql2FileRunner)

    def test_runner_mysql2kafka(self):
        self.start_runner(Mysql2KafkaRunner)
#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.sqlserver import Sqlserver2MysqlRunner, Sqlserver2OracleRunner, \
    Sqlserver2HiveRunner, Sqlserver2HDFSRunner, Sqlserver2PostgresqlRunner, Sqlserver2HbaseRunner, \
    Sqlserver2SqlserverRunner, Sqlserver2FtpRunner, Sqlserver2FileRunner, Sqlserver2KafkaRunner


class TestRunnerSqlserver(TestRunnerManager):
    def test_runner_sqlserver2sqlserver(self):
        self.start_runner(Sqlserver2SqlserverRunner)

    def test_runner_sqlserver2mysql(self):
        self.start_runner(Sqlserver2MysqlRunner)

    def test_runner_sqlserver2oracle(self):
        self.start_runner(Sqlserver2OracleRunner)

    def test_runner_sqlserver2hive(self):
        self.start_runner(Sqlserver2HiveRunner)

    def test_runner_sqlserver2HDFS(self):
        self.start_runner(Sqlserver2HDFSRunner)

    def test_runner_sqlserver2postgresql(self):
        self.start_runner(Sqlserver2PostgresqlRunner)

    def test_runner_sqlserver2Hbase(self):
        self.start_runner(Sqlserver2HbaseRunner)

    def test_runner_sqlserver2ftp(self):
        self.start_runner(Sqlserver2FtpRunner)

    def test_runner_sqlserver2file(self):
        self.start_runner(Sqlserver2FileRunner)

    def test_runner_sqlserver2kafka(self):
        self.start_runner(Sqlserver2KafkaRunner)
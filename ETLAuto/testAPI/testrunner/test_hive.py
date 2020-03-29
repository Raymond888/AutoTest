#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.hive import Hive2MysqlRunner, Hive2OracleRunner, Hive2HiveRunner, \
    Hive2HDFSRunner, Hive2PostgresqlRunner, Hive2HbaseRunner, Hive2SqlserverRunner, Hive2FtpRunner, \
    Hive2FileRunner, Hive2KafkaRunner


class TestRunnerHive(TestRunnerManager):
    def test_runner_hive2mysql(self):
        self.start_runner(Hive2MysqlRunner)

    def test_runner_hive2oracle(self):
        self.start_runner(Hive2OracleRunner)

    def test_runner_hive2hive(self):
        self.start_runner(Hive2HiveRunner)

    def test_runner_hive2HDFS(self):
        self.start_runner(Hive2HDFSRunner)

    def test_runner_hive2postgresql(self):
        self.start_runner(Hive2PostgresqlRunner)

    def test_runner_hive2Hbase(self):
        self.start_runner(Hive2HbaseRunner)

    def test_runner_hive2sqlserver(self):
        self.start_runner(Hive2SqlserverRunner)

    def test_runner_hive2ftp(self):
        self.start_runner(Hive2FtpRunner)

    def test_runner_hive2file(self):
        self.start_runner(Hive2FileRunner)

    def test_runner_hive2kafka(self):
        self.start_runner(Hive2KafkaRunner)
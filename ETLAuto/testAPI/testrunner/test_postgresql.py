#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.postgresql import Postgresql2MysqlRunner, Postgresql2OracleRunner, \
    Postgresql2HiveRunner, Postgresql2HDFSRunner, Postgresql2PostgresqlRunner, Postgresql2HbaseRunner, \
    Postgresql2SqlserverRunner, Postgresql2FtpRunner, Postgresql2FileRunner, Postgresql2KafkaRunner


class TestRunnerPostgresql(TestRunnerManager):
    def test_runner_postgresql2postgresql(self):
        self.start_runner(Postgresql2PostgresqlRunner)
        
    def test_runner_postgresql2mysql(self):
        self.start_runner(Postgresql2MysqlRunner)

    def test_runner_postgresql2oracle(self):
        self.start_runner(Postgresql2OracleRunner)

    def test_runner_postgresql2hive(self):
        self.start_runner(Postgresql2HiveRunner)

    def test_runner_postgresql2HDFS(self):
        self.start_runner(Postgresql2HDFSRunner)

    def test_runner_postgresql2Hbase(self):
        self.start_runner(Postgresql2HbaseRunner)

    def test_runner_postgresql2sqlserver(self):
        self.start_runner(Postgresql2SqlserverRunner)

    def test_runner_postgresql2ftp(self):
        self.start_runner(Postgresql2FtpRunner)

    def test_runner_postgresql2file(self):
        self.start_runner(Postgresql2FileRunner)

    def test_runner_postgresql2kafka(self):
        self.start_runner(Postgresql2KafkaRunner)
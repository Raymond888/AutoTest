#!/usr/bin/python
# -*- coding: utf-8 -*-

from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.sybase import Sybase2MysqlRunner, Sybase2OracleRunner, Sybase2HiveRunner, \
    Sybase2HDFSRunner, Sybase2PostgresqlRunner, Sybase2SqlserverRunner


class TestRunnerSybase(TestRunnerManager):
    def test_runner_sybase2mysql(self):
        self.start_runner(Sybase2MysqlRunner)

    def test_runner_sybase2oracle(self):
        self.start_runner(Sybase2OracleRunner)

    def test_runner_sybase2hive(self):
        self.start_runner(Sybase2HiveRunner)

    def test_runner_sybase2HDFS(self):
        self.start_runner(Sybase2HDFSRunner)

    def test_runner_sybase2postgresql(self):
        self.start_runner(Sybase2PostgresqlRunner)

    def test_runner_sybase2sqlserver(self):
        self.start_runner(Sybase2SqlserverRunner)
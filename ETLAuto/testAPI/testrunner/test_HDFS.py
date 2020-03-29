#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.HDFS import HDFS2FileCsv2CsvRunner, HDFS2FileText2TextRunner, \
    HDFS2FileOrc2OrcRunner, HDFS2FtpRunner, HDFS2HDFSCsv2CsvRunner, HDFS2KafkaRunner, HDFS2OracleRunner, \
    HDFS2PostgresqlRunner, HDFS2SqlserverRunner, HDFS2MysqlRunner



class TestRunnerHDFS(TestRunnerManager):
    def test_runner_HDFS2file_csv2csv(self):
        self.start_runner(HDFS2FileCsv2CsvRunner)

    def test_runner_HDFS2file_text2text(self):
        self.start_runner(HDFS2FileText2TextRunner)

    def test_runner_HDFS2file_orc2orc(self):
        self.start_runner(HDFS2FileOrc2OrcRunner)

    def test_runner_HDFS2ftp(self):
        self.start_runner(HDFS2FtpRunner)

    def test_runner_HDFS2HDFSCsv2Csv(self):
        self.start_runner(HDFS2HDFSCsv2CsvRunner)

    def test_runner_HDFS2kafka(self):
        self.start_runner(HDFS2KafkaRunner)

    def test_runner_HDFS2oracle(self):
        self.start_runner(HDFS2OracleRunner)

    def test_runner_HDFS2postgresql(self):
        self.start_runner(HDFS2PostgresqlRunner)

    def test_runner_HDFS2sqlserver(self):
        self.start_runner(HDFS2SqlserverRunner)

    def test_runner_HDFS2mysql(self):
        self.start_runner(HDFS2MysqlRunner)

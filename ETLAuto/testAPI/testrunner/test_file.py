#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.file import File2FileCsv2CsvRunner, File2FileRaw2RawRunner, File2HDFSCsv2CsvRunner, \
    File2HDFSRaw2RawRunner, File2KafkaCsv2CsvRunner, File2KafkaRaw2RawRunner, File2SqlserverRunner, File2FtpCsv2CsvRunner, \
    File2FtpRaw2RawRunner, File2MysqlRunner, File2OracleRunner, File2HiveRunner, File2HbaseRunner



class TestRunnerFile(TestRunnerManager):
    # def test_runner_file2file_csv2csv(self):
    #     self.start_runner(File2FileCsv2CsvRunner)
    #
    # def test_runner_file2file_raw2raw(self):
    #     self.start_runner(File2FileRaw2RawRunner)

    def test_runner_file2HDFS_csv2csv(self):
        self.start_runner(File2HDFSCsv2CsvRunner)

    def test_runner_file2HDFS_raw2raw(self):
        self.start_runner(File2HDFSRaw2RawRunner)

    def test_runner_file2kafka_csv2csv(self):
        self.start_runner(File2KafkaCsv2CsvRunner)

    def test_runner_file2kafka_raw2raw(self):
        self.start_runner(File2KafkaRaw2RawRunner)

    def test_runner_file2sqlserver(self):
        self.start_runner(File2SqlserverRunner)

    def test_runner_file2ftp_csv2csv(self):
        self.start_runner(File2FtpCsv2CsvRunner)

    def test_runner_file2ftp_raw2raw(self):
        self.start_runner(File2FtpRaw2RawRunner)

    def test_runner_file2mysql(self):
        self.start_runner(File2MysqlRunner)

    def test_runner_file2oracle(self):
        self.start_runner(File2OracleRunner)

    def test_runner_file2hive(self):
        self.start_runner(File2HiveRunner)

    def test_runner_file2Hbase(self):
        self.start_runner(File2HbaseRunner)

#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.kafka import Kafka2FileRunner, Kafka2HDFSRunner, Kafka2KafkaRunner, \
    Kafka2SqlserverRunner, Kafka2FtpRunner


class TestRunnerKafka(TestRunnerManager):
    # def test_runner_kafka2kafka(self):
    #     self.start_runner(Kafka2KafkaRunner)

    def test_runner_kafka2file(self):
        self.start_runner(Kafka2FileRunner)

    def test_runner_kafka2HDFS(self):
        self.start_runner(Kafka2HDFSRunner)

    # def test_runner_kafka2sqlserver(self):
    #     self.start_runner(Kafka2SqlserverRunner)

    def test_runner_kafka2ftp(self):
        self.start_runner(Kafka2FtpRunner)

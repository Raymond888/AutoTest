#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.ftp import Ftp2HDFSRunner, Ftp2KafkaRunner


class TestRunnerFtp(TestRunnerManager):
    def test_runner_ftp2HDFS(self):
        self.start_runner(Ftp2HDFSRunner)

    def test_runner_ftp2kafka(self):
        self.start_runner(Ftp2KafkaRunner)

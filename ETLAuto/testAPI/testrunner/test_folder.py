#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.folder import Folder2HDFSRunner


class TestRunnerFolder(TestRunnerManager):
    def test_runner_folder2HDFS(self):
        self.start_runner(Folder2HDFSRunner)

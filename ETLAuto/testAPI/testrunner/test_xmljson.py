#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.objects.source.xmljson import Xml2MysqlRunner, Xml2OracleRunner, Json2MysqlRunner, \
    Json2OracleRunner, Xml2HiveRunner, Json2HiveRunner


class TestRunnerXMLJSON(TestRunnerManager):
    def test_runner_xml2mysql(self):
        self.start_runner(Xml2MysqlRunner)

    def test_runner_xml2oracle(self):
        self.start_runner(Xml2OracleRunner)

    def test_runner_xml2hive(self):
        self.start_runner(Xml2HiveRunner)

    def test_runner_json2mysql(self):
        self.start_runner(Json2MysqlRunner)

    def test_runner_json2oracle(self):
        self.start_runner(Json2OracleRunner)

    def test_runner_json2hive(self):
        self.start_runner(Json2HiveRunner)

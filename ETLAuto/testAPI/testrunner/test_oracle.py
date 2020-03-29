#!/usr/bin/python
# -*- coding: utf-8 -*-


import os
import sys


from ETLAuto.common.remote import ParamikoConnector
from ETLAuto.objects.source.oracle import Oracle2OracleRunner, Oracle2MysqlRunner, Oracle2HiveRunner, \
    Oracle2HDFSRunner, Oracle2PostgresqlRunner, Oracle2HbaseRunner, Oracle2SqlserverRunner, Oracle2FtpRunner, \
    Oracle2FileRunner, Oracle2KafkaRunner, Oracle2HiveRunnerRowID, Oracle2HiveRunnerFiled, \
    Oracle2HiveRunnerWhere, Oracle2HiveRunnerLack, Oracle2HiveRunnerView, Oracle2HiveRunnerTransformerFilter, \
    Oracle2HiveRunnerTransformerDate, Oracle2HiveRunnerTransformerReplace, Oracle2HiveRunnerTransformerMerge, \
    Oracle2HiveRunnerTransformerCustom, Oracle2HiveRunnerPartitionDynamic, Oracle2HiveRunnerPartitionStatic, \
    Oracle2HiveRunnerStorage, Oracle2HiveRunnerExcel, Oracle2HiveRunnerHAMode, Oracle2HiveRunnerGlob, \
    Oracle2HiveRunnerVar, Oracle2OracleRunnerNVarChar2, IncreaseOracle11g2HiveRunner, IncreaseOracle18c2HiveRunner
from ETLAuto.settings import ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS, SERVER_SETTINGS, XSTREAM_SCRIPT_PATH
from ETLAuto.testAPI.testrunner.base import TestRunnerManager
from ETLAuto.common.database import OracleHandler
from ETLAuto.config.statements import sql_create, sql_insert, sql_prepare, sql_data
from ETLAuto.utils.commonutils import get_unique_id


def create_new_table(table, **oracle_settings):
    oracle = OracleHandler()
    oracle.exec_commit(sql_create.format(user=oracle_settings['READER']['user'], table=table))

    # if oracle.exec_commit(sql_insert.format(user=oracle_settings['READER']['user'], table=table)):
    #     return True

    if oracle.exec_many(sql_prepare.format(user=oracle_settings['READER']['user'], table=table), sql_data):
        return True

    oracle.close()
    return False



def auto_set_xstream(table, **oracle_settings):
    statement = '{xstream_script_path} {oracle_user} {oracle_password} {oracle_table}' \
        .format(xstream_script_path=XSTREAM_SCRIPT_PATH,
                oracle_user=oracle_settings['READER']['user'],
                oracle_password=oracle_settings['READER']['password'],
                oracle_table=table
                )
    pc = ParamikoConnector(username=SERVER_SETTINGS['username'],
                           password=SERVER_SETTINGS['password'],
                           host=oracle_settings['READER']['host']
                           )
    sys.stdout.write(statement+'...\n')
    return pc.ssh_execute(statement)


class TestRunnerOracle(TestRunnerManager):
    def test_runner_oracle11g2hive_increase(self):
        table = 'xstream_' + get_unique_id()[8:]
        os.environ['xtream_table'] = table

        sys.stdout.write('creating table {}...'.format(table))
        res = create_new_table(table, **ORACLE11g218c_SETTINGS)
        if not res:
            raise ValueError('create table fail')
        print('done')
        sys.stdout.write('auto set xstream {}...'.format(table))
        res = auto_set_xstream(table, **ORACLE11g218c_SETTINGS)
        if not res:
            raise ValueError('ssh execute response nothing')
        sys.stdout.write(res)
        self.start_runner(IncreaseOracle11g2HiveRunner)
        del os.environ['xtream_table']

    # def test_runner_oracle18c2hive_increase(self):
    #     table = 'xstream_test_' + get_unique_id()
    #     os.environ['xtream_table'] = table
    #
    #     res = create_new_table(table, **ORACLE18c218c_SETTINGS)
    #     if not res:
    #         raise ValueError('create table fail')
    #
    #     res = auto_set_xstream(table, **ORACLE18c218c_SETTINGS)
    #     if not res:
    #         raise ValueError('ssh execute response nothing')
    #
    #     self.start_runner(IncreaseOracle18c2HiveRunner)
    #     del os.environ['xtream_table']

    def test_runner_oracle11g2mysql(self):
        self.start_runner(Oracle2MysqlRunner)

    def test_runner_oracle11g2oracle18c(self):
        self.start_runner(Oracle2OracleRunner)

    def test_runner_oracle18c218c_nvarchar2(self):
        self.start_runner(Oracle2OracleRunnerNVarChar2)

    def test_runner_oracle11g2hive(self):
        self.start_runner(Oracle2HiveRunner)

    def test_runner_oracle11g2hive_rowid(self):
        self.start_runner(Oracle2HiveRunnerRowID)

    def test_runner_oracle11g2hive_filed(self):
        self.start_runner(Oracle2HiveRunnerFiled)

    def test_runner_oracle11g2hive_where(self):
        self.start_runner(Oracle2HiveRunnerWhere)

    def test_runner_oracle11g2hive_lack(self):
        self.start_runner(Oracle2HiveRunnerLack)

    def test_runner_oracle11g2hive_view(self):
        self.start_runner(Oracle2HiveRunnerView)

    def test_runner_oracle11g2hive_transformer_filter(self):
        self.start_runner(Oracle2HiveRunnerTransformerFilter)

    def test_runner_oracle11g2hive_transformer_date(self):
        self.start_runner(Oracle2HiveRunnerTransformerDate)

    def test_runner_oracle11g2hive_transformer_replace(self):
        self.start_runner(Oracle2HiveRunnerTransformerReplace)

    def test_runner_oracle11g2hive_transformer_merge(self):
        self.start_runner(Oracle2HiveRunnerTransformerMerge)

    def test_runner_oracle11g2hive_transformer_custom(self):
        self.start_runner(Oracle2HiveRunnerTransformerCustom)

    def test_runner_oracle11g2hive_partition_dynamic(self):
        self.start_runner(Oracle2HiveRunnerPartitionDynamic)

    def test_runner_oracle11g2hive_partition_static(self):
        self.start_runner(Oracle2HiveRunnerPartitionStatic)

    def test_runner_oracle11g2hive_storage(self):
        self.start_runner(Oracle2HiveRunnerStorage)

    def test_runner_oracle11g2hive_excel(self):
        self.start_runner(Oracle2HiveRunnerExcel)

    def test_runner_oracle11g2hive_HAMode(self):
        self.start_runner(Oracle2HiveRunnerHAMode)

    def test_runner_oracle11g2hive_glob(self):
        self.start_runner(Oracle2HiveRunnerGlob)

    def test_runner_oracle11g2hive_download(self):
        self.download_template()

    def test_runner_oracle11g2hive_var(self):
        self.start_runner(Oracle2HiveRunnerVar)

    def test_runner_oracle11g2HDFS(self):
        self.start_runner(Oracle2HDFSRunner)

    def test_runner_oracle11g2postgresql(self):
        self.start_runner(Oracle2PostgresqlRunner)

    def test_runner_oracle11g2Hbase(self):
        self.start_runner(Oracle2HbaseRunner)

    def test_runner_oracle11g2sqlserver(self):
        self.start_runner(Oracle2SqlserverRunner)

    def test_runner_oracle11g2ftp(self):
        self.start_runner(Oracle2FtpRunner)

    def test_runner_oracle11g2file(self):
        self.start_runner(Oracle2FileRunner)

    def test_runner_oracle11g2kafka(self):
        self.start_runner(Oracle2KafkaRunner)

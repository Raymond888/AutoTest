#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.settings import WARETYPE
from ETLAuto.config.builder.base import RunnerConfig, get_oracle_settings
from ETLAuto.config.builder.warehouse import Mysql, Oracle, Postgresql, Sqlserver, Hive, HDFS, Hbase, Ftp, \
    File, Kafka, IncreaseOracle, IncreaseHive
from ETLAuto.settings import MYSQL_SETTINGS, ORACLE11g218c_SETTINGS, POSTGRESQL_SETTINGS, SQLSERVER_SETTINGS, \
    HIVE_SETTINGS, HBASE_SETTINGS, HDFS_SETTINGS, SAMB_SETTINGS, FILE_SETTINGS, KAFKA_SETTINGS, ORACLE18c218c_SETTINGS


#################OracleConfigs##################
class Oracle2Oracle(RunnerConfig):
    def __init__(self,
                 src='11g',
                 dst='18c',
                 split_pk=None,
                 where=None,
                 lack=None,
                 view=None,
                 filter=None,
                 date=None,
                 replace=None,
                 merge=None,
                 custom=None,
                 dynamic=None,
                 static=None,
                 storage='parquet',
                 excel=None,
                 HAMode='false',
                 glob=False,
                 var=None,
                 nvarchar=None):
        super(Oracle2Oracle, self).__init__()
        self.src = src
        self.dst = dst
        self.split_pk = split_pk
        self.where = where
        self.lack = lack
        self.view = view
        self.filter = filter
        self.date = date
        self.replace = replace
        self.merge = merge
        self.custom = custom
        self.dynamic = dynamic
        self.static = static
        self.storage = storage
        self.excel = excel
        self.HAMode = HAMode
        self.glob = glob
        self.var = var
        self.nvarchar = nvarchar

    @property
    def wh_reader(self):
        oracle_settings = get_oracle_settings(self.src, self.dst)

        return Oracle(mapping_from=oracle_settings,
                      src=self.src,
                      dst=self.dst,
                      split_pk=self.split_pk,
                      where=self.where,
                      lack=self.lack,
                      view=self.view,
                      excel=self.excel,
                      nvarchar=self.nvarchar)

    @property
    def wh_transformer(self):
        return Oracle(src=self.src,
                      dst=self.dst,
                      filter=self.filter,
                      date=self.date,
                      replace=self.replace,
                      merge=self.merge,
                      custom=self.custom)

    @property
    def wh_writer(self):
        oracle_settings = get_oracle_settings(self.src, self.dst)

        return Oracle(mapping_from=oracle_settings,
                      mapping_to=oracle_settings,
                      src=self.src,
                      dst=self.dst,
                      reader_type=WARETYPE[0])


class Oracle2Mysql(Oracle2Oracle):
    @property
    def wh_writer(self):
        return Mysql(mapping_from=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[0])


class Oracle2Hive(Oracle2Oracle):
    def __init__(self,
                 split_pk=None,
                 where=None,
                 lack=None,
                 view=None,
                 filter=None,
                 date=None,
                 replace=None,
                 merge=None,
                 custom=None,
                 dynamic=None,
                 static=None,
                 storage='parquet',
                 excel=None,
                 HAMode='false',
                 glob=False,
                 var=None):
        super(Oracle2Hive, self).__init__(split_pk=split_pk,
                                          where=where,
                                          lack=lack,
                                          view=view,
                                          filter=filter,
                                          date=date,
                                          replace=replace,
                                          merge=merge,
                                          custom=custom,
                                          dynamic=dynamic,
                                          static=static,
                                          storage=storage,
                                          excel=excel,
                                          HAMode=HAMode,
                                          glob=glob,
                                          var=var)

    @property
    def wh_writer(self):
        return Hive(mapping_from=ORACLE11g218c_SETTINGS,
                    mapping_to=HIVE_SETTINGS,
                    reader_type=WARETYPE[0],
                    view=self.view,
                    filter=self.filter,
                    dynamic=self.dynamic,
                    static=self.static,
                    storage=self.storage,
                    excel=self.excel,
                    HAMode=self.HAMode,
                    glob=self.glob,
                    var=self.var)


class IncreaseOracle11g2Hive(Oracle2Oracle):
    def __init__(self):
        super(IncreaseOracle11g2Hive, self).__init__()
        self.collector_type = 'increment'

    @property
    def wh_reader(self):
        return IncreaseOracle(mapping_from=ORACLE11g218c_SETTINGS, src='11g')

    @property
    def wh_writer(self):
        return IncreaseHive(mapping_from=ORACLE11g218c_SETTINGS, mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[0])


class IncreaseOracle18c2Hive(Oracle2Oracle):
    def __init__(self):
        super(IncreaseOracle18c2Hive, self).__init__()
        self.collector_type = 'increment'

    @property
    def wh_reader(self):
        return IncreaseOracle(mapping_from=ORACLE18c218c_SETTINGS, src='18c')

    @property
    def wh_writer(self):
        return IncreaseHive(mapping_from=ORACLE18c218c_SETTINGS, mapping_to=HIVE_SETTINGS, reader_type=WARETYPE[0])



class Oracle2HDFS(Oracle2Oracle):
    @property
    def wh_reader(self):
        return Oracle(mapping_from=ORACLE11g218c_SETTINGS, mapping_to=HDFS_SETTINGS, table='normal')

    @property
    def wh_writer(self):
        return HDFS(reader_type=WARETYPE[0], writer_format='csv')


class Oracle2Postgresql(Oracle2Oracle):
    @property
    def wh_writer(self):
        return Postgresql(mapping_from=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[0])


class Oracle2Hbase(Oracle2Oracle):
    @property
    def wh_reader(self):
        return Oracle(mapping_from=ORACLE11g218c_SETTINGS, mapping_to=HBASE_SETTINGS, table='normal')

    @property
    def wh_writer(self):
        return Hbase(mapping_from=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[0], column='normal')


class Oracle2Sqlserver(Oracle2Oracle):
    @property
    def wh_writer(self):
        return Sqlserver(mapping_from=ORACLE11g218c_SETTINGS, reader_type=WARETYPE[0])


class Oracle2Ftp(Oracle2Oracle):
    @property
    def wh_reader(self):
        return Oracle(mapping_from=ORACLE11g218c_SETTINGS, mapping_to=SAMB_SETTINGS, table='normal')

    @property
    def wh_writer(self):
        return Ftp(reader_type=WARETYPE[0], writer_format='csv')


class Oracle2File(Oracle2Oracle):
    @property
    def wh_reader(self):
        return Oracle(mapping_from=ORACLE11g218c_SETTINGS, mapping_to=FILE_SETTINGS, table='normal')

    @property
    def wh_writer(self):
        return File(reader_type=WARETYPE[0], writer_format='csv')


class Oracle2Kafka(Oracle2Oracle):
    @property
    def wh_writer(self):
        return Kafka(reader_type=WARETYPE[0], writer_format='csv')

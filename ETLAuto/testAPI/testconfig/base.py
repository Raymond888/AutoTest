#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.objects.base import BaseDatasource


class TestConfigManager(object):
    def add_data(self, json_data):
        ds = BaseDatasource()
        res_data = ds.add_datasource(json_data)
        return res_data


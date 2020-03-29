#!/usr/bin/python
# -*- coding: utf-8 -*-


import random
import requests

from ETLAuto.config.urls import URL
from ETLAuto.objects.base import BaseObject


class API(BaseObject):
    def __init__(self):
        super(API, self).__init__()

    def get_agent_list(self):
        url = self.host + getattr(URL, 'agent_list')
        res = requests.get(url, headers=self.headers)
        assert res.status_code == 200, res

        res_data = res.json().get('data', {}).get('agents', [])
        ip_list = [item.get('ip') for item in res_data if item.get('state') == 'online']
        return ip_list

    def get_table_columns(self, json_data):
        url = self.host + getattr(URL, 'table_columns')
        res = requests.post(url, headers=self.headers, json=json_data)
        assert res.status_code == 200, res

        column_list = res.json().get('data', {}).get('columns', [])
        return column_list

    def get_hbase_column_info(self, json_data):
        url = self.host + getattr(URL, 'hbase_column_info')
        res = requests.post(url, headers=self.headers, json=json_data)
        assert res.status_code == 200, res

        column_list = res.json().get('data', {})
        name_list = [item.get('colname') for item in column_list]
        type_list = [random.choice(item.get('hbasetype')) for item in column_list]
        column_info = {'name': name_list,
                       'type': type_list,
                       }
        return column_info

    def get_sender_options(self, json_data):
        url = self.host + getattr(URL, 'sender_options')
        res = requests.post(url, headers=self.headers, json=json_data)
        assert res.status_code == 200, res

        return res.json().get('data', {})

    def get_hive_writer_option(self, json_data):
        sender_options = self.get_sender_options(json_data)
        hive_writer_option = sender_options.get('hivewriter')
        return hive_writer_option


#!/usr/bin/python
# -*- coding: utf-8 -*-

from ETLAuto.settings import BASE_URL


class URL(object):
    # for API
    add_runner_auto = '/ds/v1/runner/configs/auto'

    # for UI
    login = 'http://10.200.60.36:8800/login?service={}/#/main/dataCollection'.format(BASE_URL)
    data_collection = 'http://10.200.60.36:8800/login?service={}/#/main/dataCollection'.format(BASE_URL)


#!/usr/bin/python
# -*- coding: utf-8 -*-


import os

from ETLAuto.objects.base import BaseFtp
from ETLAuto.settings import SAMB_SETTINGS
from ETLAuto.testAPI.testconfig.base import TestConfigManager
from ETLAuto.utils.commonutils import get_unique_id
from ETLAuto.utils.paramizeutils import SettingsParam


class TestUpload(TestConfigManager):
    def test_add_ftp(self):
        ftp_settings = SettingsParam(SAMB_SETTINGS)
        json_data = {'name': 'ftp' + get_unique_id(),
                     'host': ftp_settings.READER.host,
                     'port': ftp_settings.READER.port,
                     'isAnonymous': False,
                     'userName': ftp_settings.READER.user,
                     'password': ftp_settings.READER.password,
                     'dataPath': os.path.dirname(ftp_settings.READER.path.sql),
                     'note': '',
                     }

        ftp = BaseFtp()
        res_data = ftp.add_ftp(json_data)
        assert res_data.get('error_code') == 0

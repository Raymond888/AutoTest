#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.objects.base import BaseAlarm
from ETLAuto.settings import ALARM_SETTINGS
from ETLAuto.testAPI.testconfig.base import TestConfigManager
from ETLAuto.utils.commonutils import get_unique_id
from ETLAuto.utils.paramizeutils import SettingsParam


class TestAlarm(TestConfigManager):
    def test_add_alarm(self):
        alarm_settings = SettingsParam(ALARM_SETTINGS)
        json_data = {'name': 'alarm' + get_unique_id(),
                     'smtp': alarm_settings.host,
                     'smtp_port': alarm_settings.port,
                     'username': alarm_settings.username,
                     'password': alarm_settings.password,
                     'note': '',
                     }

        alarm = BaseAlarm()
        res_data = alarm.add_alarm(json_data)
        assert res_data.get('error_code') == 0

#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.objects.base import BaseVariable
from ETLAuto.testAPI.testconfig.base import TestConfigManager
from ETLAuto.utils.commonutils import get_unique_id


class TestVariable(TestConfigManager):
    def test_add_variable(self):
        json_data = {'key': 'k' + get_unique_id(),
                     'value': 'v' + get_unique_id(),
                     'notes': '',
                     }
        v = BaseVariable()
        res = v.add_variable(json_data)

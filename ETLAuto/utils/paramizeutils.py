#!/usr/bin/python
# -*- coding: utf-8 -*-


class BaseParam(object):
    pass


class SettingsParam(BaseParam):
    def __init__(self, settings):
        for k, v in settings.items():
            if isinstance(v, (list, tuple)):
                setattr(self, k, [SettingsParam(x) if isinstance(x, dict) else x for x in v])
            else:
                setattr(self, k, SettingsParam(v) if isinstance(v, dict) else v)



def encode_json(json_data, encoding='UTF-8'):
    for k, v in json_data.items():
        if isinstance(v, (list, tuple)):
            json_data[k] = [encode_json(x) if isinstance(x, dict) else x.encode(encoding) for x in v]
        else:
            json_data[k] = encode_json(v) if isinstance(v, dict) else v.encode(encoding)

    return json_data
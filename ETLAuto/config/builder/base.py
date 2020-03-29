#!/usr/bin/python
# -*- coding: utf-8 -*-


from ETLAuto.utils.commonutils import uuid_, get_datetime
from ETLAuto.settings import ORACLE11g218c_SETTINGS, ORACLE18c218c_SETTINGS


class RunnerConfig(object):
    def __init__(self):
        self.uuid = get_datetime()
        self.collector_type = 'full'

    @property
    def config(self):
        info = {'config': {'job': self.job,
                           'alarm': self.alarm,
                           },
                'name': self.name,
                'exec_type': 'start',  # start, not_start
                'note': '',
                'collector_type': self.collector_type,
                'agent_ids': []
                }
        return info

    @property
    def name(self):
        return '_'.join(['bvt', type(self).__name__, self.uuid])[:40]

    @property
    def alarm(self):
        return {'enable': False,
                'contacts': [],
                }

    @property
    def job(self):
        settings_data = {'speed': {'byte': '',
                                   'record': '',
                                   },
                         'taskFailed': False,
                         }

        if self.collector_type == 'full':
            settings_data.update({'openOffset': False})
        else:
            settings_data.update({'runtime': '2:0:0',
                                  'supportIncrement': True,
                                  })

        return {'setting': settings_data,
                'content': self.content
                }

    @property
    def content(self):
        return [{'reader': self.reader,
                 'transformer': self.transformer,
                 'writer': self.writer,
                 }]

    @property
    def reader(self):
        return self.wh_reader.reader

    @property
    def transformer(self):
        return self.wh_transformer.transformer

    @property
    def writer(self):
        return self.wh_writer.writer

    @property
    def wh_reader(self):
        return NotImplemented

    @property
    def wh_transformer(self):
        return NotImplemented

    @property
    def wh_writer(self):
        return NotImplemented


def get_oracle_settings(src, dst):
    if src == '11g' and dst == '18c':
        oracle_settings = ORACLE11g218c_SETTINGS
    elif src == '18c' and dst == '18c':
        oracle_settings = ORACLE18c218c_SETTINGS
    else:
        raise ValueError('oracle version not match')
    return oracle_settings

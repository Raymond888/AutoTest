#!/usr/bin/python
# -*- coding: utf-8 -*-

# this module prepare test data set by manipulate and process
# reference link https://faker.readthedocs.io/en/master/locales/zh_CN.html

from faker import Faker


class DataSet(object):
    def __init__(self, lang='en_US'):
        '''
        :param lang: zh_CN, zh_TW, en_US, en_GB, de_DE, ja_JP, ko_KR, fr_FR
        '''
        self.lang = lang

    def faker(self):
        '''
        :return: faker, faker.name(), faker.address(), ...
        '''
        faker = Faker(self.lang)
        return faker

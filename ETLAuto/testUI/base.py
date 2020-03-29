# #!/usr/bin/python
# # -*- coding: utf-8 -*-
#
#
# import pytest
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.keys import Keys
#
# from ETLAuto.config.urls import URL
# from ETLAuto.config.accounts import ACCOUNT
#
#
# #Fixture for Firefox, Chrome
# @pytest.fixture(params=['chrome', 'firefox'], scope='class')
# def driver_init(request):
#     if request.param == 'chrome':
#         web_driver = webdriver.Chrome()
#     elif request.param == 'firefox':
#         web_driver = webdriver.Firefox()
#     else:
#         raise ValueError('param not support')
#
#     request.cls.driver = web_driver
#     yield
#     web_driver.close()
#
#
# @pytest.mark.usefixtures('driver_init')
# class TestPageManager(object):
#     def setup_class(self, selenium):
#         selenium.get(URL.login)
#         selenium.implicitly_wait(10)
#         selenium.maximize_window()
#         selenium.find_element_by_id('username').send_keys(ACCOUNT['name'])
#         selenium.find_element_by_id('inputPassword').send_keys(ACCOUNT['password'])
#         selenium.find_element_by_id('loginBtn').click()
#         return selenium
#
#     def teardown_class(self, selenium):
#         pass

#!/usr/bin/python
# -*- coding: utf-8 -*-


import os
import yagmail
from settings import MODULE_NAME
from utils.commonutils import safe_request, get_datetime


class Email:
    def __init__(self, user, password, host):
        self.user = user
        self.password = password
        self.host = host

        self.connect = self._connect

    def __del__(self):
        del self.connect

    @property
    def _connect(self):
        connect = yagmail.SMTP(user=self.user, password=self.password, host=self.host)
        return connect

    # @safe_request
    def send_email(self, to, subject=None, contents=None, attachments=None, cc=None):
        if not subject:
            subject = 'AutoTest_report_' + get_datetime()

        if not contents:
            contents = ['This is xxx autotest report, please see attachment for details.<br>\
            <p>Test Environment: {}</p>'.format(os.environ[MODULE_NAME])]

        if not cc:
            cc = ['xxx@xxx.com']

        self.connect.send(to=to, subject=subject, contents=contents, attachments=attachments, cc=cc)


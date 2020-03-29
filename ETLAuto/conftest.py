#!/usr/bin/python
# -*- coding: utf-8 -*-


import os
import traceback

import pytest
from py._xmlgen import html
from ETLAuto.settings import MODULE_NAME


@pytest.fixture(scope='session', autouse=True)
def configure_html_environment(request):
    if not MODULE_NAME in os.environ:
        from ETLAuto.settings import BASE_URL
        os.environ[MODULE_NAME] = BASE_URL

    request.config._metadata.pop('Base URL', None)
    request.config._metadata.pop('Capabilities', None)
    request.config._metadata.pop('Driver', None)
    request.config._metadata.pop('Plugins', None)
    request.config._metadata.update({'Product/Project': 'DataSocket',
                                     'Test Environment': os.environ[MODULE_NAME],
                                     })


@pytest.mark.optionalhook
def pytest_html_results_table_header(cells):
    cells.insert(1, html.th('Error'))
    cells.pop()


@pytest.mark.optionalhook
def pytest_html_results_table_row(report, cells):
    cells.insert(1, html.td(report.error))
    cells.pop()


@pytest.mark.hookwrapper
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    report.error = 'None'

    if report.when == 'call' or report.when == "setup":
        xfail = hasattr(report, 'wasxfail')
        if (report.skipped and xfail) or (report.failed and not xfail):
            report.error = catch_exception()


def catch_exception():
    return traceback.format_exc().splitlines()[-1]

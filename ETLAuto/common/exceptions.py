#!/usr/bin/python
# -*- coding: utf-8 -*-



class ArgumentsError(Exception):
    def __init__(self, *args):
        super(ArgumentsError, self).__init__(*args)


class ConnectionError(Exception):
    def __init__(self, *args):
        super(ConnectionError, self).__init__(*args)

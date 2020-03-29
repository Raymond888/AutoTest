#!/usr/bin/env python:
#-*- coding: utf-8 -*-


import codecs
import json
import os
import math
import time
import shutil
import functools
import hashlib
import requests
import uuid


def make_dirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


def path_exists(path):
    if os.path.exists(path):
        return True
    return False


def join_path(*args):
    return functools.reduce(lambda x, y: os.path.join(x, y), args)


def copy_file(src_path, dst_path):
    if not os.path.exists(dst_path):
        shutil.copy(src_path, dst_path)


def move_file(src_path, dst_path):
    if not os.path.exists(dst_path):
        shutil.move(src_path, dst_path)


def log_row(fn, mode='w', *args):
    with codecs.open(fn, mode, encoding='utf-8') as f:
        f.write('\n'.join(args).strip() + '\n')


def log_column(fn, mode='w', *args):
    with codecs.open(fn, mode, encoding='utf-8') as f:
        f.write('\t'.join(args).strip() + '\n')


def json_dump(json_data, fn):
    with codecs.open(fn, 'w', encoding='utf-8') as f:
        f.write(json.dumps(json_data, ensure_ascii=False, indent=4, sort_keys=True) + '\n')


def json_load(fn):
    with codecs.open(fn, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    return json_data


def get_lines(txt):
    with codecs.open(txt, 'rb', encoding='utf-8', errors='ignore') as f:
        for line in f:
            yield line.strip()


def safe_request(call):
    def _safe_request(*args, **kwargs):
        for i in range(5):
            try:
                return call(*args, **kwargs)
            except:
                time.sleep(pow(2, i))
        print('Error: Failed AutoReconnect!')

    return _safe_request


def get_datetime():
    return time.strftime("%Y%m%d%H%M%S", time.localtime())


def full2half(line):
    new_line = ''
    for uchar in line:
        inside_code = ord(uchar)
        if inside_code == 12288:
            inside_code = 32
        elif (inside_code >= 65281 and inside_code <= 65374):
            inside_code -= 65248
        new_line += chr(inside_code)
    return new_line


def get_file_realpath(src, *tar):
    for root, _, files in os.walk(src):
        for fn in files:
            fn_name, fn_ext = os.path.splitext(fn)
            if fn_ext.lower() not in tar:
                continue

            yield os.path.join(root, fn)


def get_file_relpath(src, *tar):
    for root, _, files in os.walk(src):
        for fn in files:
            fn_name, fn_ext = os.path.splitext(fn)
            if fn_ext.lower() not in tar:
                continue

            yield os.path.relpath(os.path.join(root, fn), src)


class SimpleConfig(object):
    def __init__(self, txt):
        self.lines = get_lines(txt)
        self.config = self._config

    @property
    def _config(self):
        config = {}

        for line in self.lines:
            segments = [l.strip() for l in line.split('\t')]
            config[segments[0]] = segments[1:]

        return config


import string, random
def generate_random_string(length=8, punct=False):
    if not punct:
        strings = ''.join([string.ascii_letters, string.digits])
    else:
        strings = ''.join([string.ascii_letters, string.digits, string.punctuation])

    if length > len(strings):
        raise ValueError('length value out of range')
    return ''.join(random.sample(list(strings), length))


def slice(sequence, num_chunk=10):
    num_piece = int(math.ceil(float(len(sequence)) / float(num_chunk)))
    return (sequence[num_chunk*n:num_chunk*(n+1)] for n in range(num_piece))


def strip_line(s):
    return s.replace('\t', ' ').replace('\n', ' ').strip()


def md5(s):
    return hashlib.new('md5', s).hexdigest()


def delete_file(file_path):
    if path_exists(file_path):
        os.remove(file_path)
        return True
    return False


def get_unique_id():
    timestamp = get_datetime()
    random_string = generate_random_string()
    return timestamp + random_string


uuid_ = str(uuid.uuid4()).replace('-', '')


from urllib3 import encode_multipart_formdata
def upload_file(url, headers, file_path, json_data=None):
    filename = os.path.basename(file_path)
    files = {'file': (filename, open(file_path,'rb').read())}
    if json_data:
        files.update(json_data)

    encode_data = encode_multipart_formdata(files)
    data, headers['Content-Type'] = encode_data
    res = requests.post(url, headers=headers, data=data)
    return res


if __name__ == '__main__':
    pass
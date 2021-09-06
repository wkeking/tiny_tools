#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
@author: ha er
@software: PyCharm
@file: xxljob_executor_python_frameless.py
@time: 2021/8/31 11:16
"""
__author__ = 'ha er'
import urllib2
from threading import Thread
import socket
import json
import os
import argparse
import time
import sys

reload(sys)
sys.setdefaultencoding('utf8')

parser = argparse.ArgumentParser()
parser.add_argument('-ht', '--host', type=str, default='172.27.192.1', dest='host')
parser.add_argument('-p', '--port', type=int, default=8081, dest='port')
parser.add_argument('-xh', '--xxlhost', type=str, required=False, dest='xxlhost', default='http://localhost:8080/xxl-job-admin')
parser.add_argument('-n', '--name', type=str, required=False, dest='name', default='python-client')
args = parser.parse_args()
HOST = args.host
PORT = args.port
XXL_JOB_ADDRESS = args.xxlhost
APP_NAME = args.name

JOB_THREAD_DICT = {}


class RequestParser:
    _blank_line = "\r\n\r\n"
    _buf_size = 1024 * 1024 * 10
    _line_mark = "\r\n"
    _content_length = "content-length"

    @classmethod
    def parse_request_message(cls, client_socket):
        request_line = None
        request_headers = {}
        request_body = None
        recv_data_buf = client_socket.recv(cls._buf_size)
        blank_line_index = recv_data_buf.index(cls._blank_line)
        request_base_data = recv_data_buf[:blank_line_index]
        request_base = request_base_data.decode()
        for index, line_data in enumerate(request_base.split(cls._line_mark)):
            if index == 0:
                request_line = line_data
            else:
                key = line_data.split(':')[0]
                value = line_data.lstrip(key).lstrip(':')
                key = key.strip(' ').lower()
                value = value.strip(' ')
                request_headers[key] = value

        if cls._content_length not in request_headers.keys():
            return request_line, request_headers, request_body

        body_start_index = blank_line_index + 4
        content_length = int(request_headers[cls._content_length])
        request_body_data = recv_data_buf[body_start_index:body_start_index + content_length]
        request_body = request_body_data.decode('utf-8')
        return request_line, request_headers, request_body

    @classmethod
    def get_http_method(cls, request_line):
        return str(request_line).split(" ")[0].upper()

    @classmethod
    def get_http_uri(cls, request_line):
        return str(request_line).split(" ")[1]


listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listen_socket.bind((HOST, PORT))
listen_socket.listen(1)
print('Serving HTTP on port %s ...' % PORT)

http_response = """HTTP/1.1 200 OK
Content-Type: application/json

%s"""
REGISTRY_TIME = 30


def registry():
    registry_param = {
        "registryGroup": "EXECUTOR",
        "registryKey": APP_NAME,
        "registryValue": 'http://' + HOST + ":" + str(PORT) + "/"
    }
    headers = {
        "connection": "Keep-Alive",
        "Content-Type": "application/json;charset=UTF-8",
        "Accept-Charset": "application/json;charset=UTF-8"
    }
    data = json.dumps(registry_param)
    registry_api_url = XXL_JOB_ADDRESS.rstrip('/') + '/api/registry'
    req = urllib2.Request(registry_api_url, data=data, headers=headers)
    while True:
        response = urllib2.urlopen(req)
        print(response.read().decode('utf-8'))
        time.sleep(REGISTRY_TIME)


def log_dir_name(logDateTime):
    time_array = time.localtime(logDateTime / 1000)
    return time.strftime('%Y-%m-%d', time_array)


def run_script_job(**json_body):
    glue_source = json_body['glueSource']
    job_id = str(json_body['jobId'])
    glue_type = json_body['glueType']
    if 'GLUE_SHELL' == glue_type:
        command = 'sh'
        script_file_name = job_id + '.sh'
    else:
        command = 'python'
        script_file_name = job_id + '.py'
    with open(script_file_name, 'wt') as f:
        f.write(glue_source)

    log_date_time = long(json_body['logDateTime'])
    log_dir = log_dir_name(log_date_time)
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)
    log_file = log_dir + '/' + str(json_body['logId']) + '.log'
    code = os.system(command + ' ' + script_file_name + ' >> ' + log_file)
    print('执行脚本%s结果%s' % (job_id, code))
    JOB_THREAD_DICT[job_id] = None
    os.remove(script_file_name)


def client_send(connection, body):
    json_body = http_response % (json.dumps(body))
    print("返回数据%s" % json_body)
    connection.sendall(json_body.encode("utf8"))
    connection.close()


# 注册本机到xxl服务端
t = Thread(target=registry)
t.start()


def beat(connection):
    body = {
        'code': 200
    }
    client_send(connection, body)


def idle_beat(connection, request_body):
    global json_body, job_id, t
    json_body = json.loads(request_body)
    job_id = json_body['jobId']
    t = JOB_THREAD_DICT[job_id]
    body = {}
    # 如果任务未执行完则返回失败
    if t:
        body['code'] = 500
        body['msg'] = 'job thread is running or has trigger queue.'
    # 如果任务已经执行完则返回成功
    else:
        body['code'] = 200
    client_send(connection, body)


def run(connection, request_body):
    body = {}
    try:
        json_body = json.loads(request_body)
        print("执行脚本请求参数:%s" % json_body)
        job_id = json_body['jobId']
        glue_type = str(json_body['glueType'])
        if not glue_type or glue_type not in ('GLUE_SHELL', 'GLUE_PYTHON'):
            body['code'] = 500
            body['msg'] = 'glueType["%s"] is not valid.' % glue_type
            client_send(connection, body)

        t = Thread(target=run_script_job, kwargs=json_body)
        t.start()
        JOB_THREAD_DICT[job_id] = t
        body['code'] = 200
        body['msg'] = 'SUCCESS'
        client_send(connection, body)
    except Exception as e:
        print(e.message)
        body['code'] = 500
        client_send(connection, body)


def kill(connection, request_body):
    json_body = json.loads(request_body)
    job_id = json_body['jobId']
    JOB_THREAD_DICT[job_id] = None
    print("中断任务%s" % job_id)
    body = {
        'code': 200
    }
    client_send(connection, body)


def log(connection, request_body):
    body = {}
    if not request_body:
        content = {
            'isEnd': True
        }
        body['code'] = 200
        body['msg'] = 'SUCCESS'
        body['content'] = content
        client_send(connection, body)
    else:
        json_body = json.loads(request_body)
        from_line_num = json_body['fromLineNum']
        log_date_time = long(json_body['logDateTim'])
        log_dir = log_dir_name(log_date_time)
        log_file = log_dir + '/' + str(json_body['logId']) + '.log'
        log_content = None
        to_line_num = None
        with open(log_file, 'rt') as f:
            for num, line_data in enumerate(f):
                if from_line_num and num + 1 >= int(from_line_num):
                    log_content = log_content + '\n' + line_data if log_content else line_data
                    to_line_num = num + 1
            content = {
                'logContent': log_content,
                'isEnd': True
            }
            if from_line_num:
                content['fromLineNum'] = int(from_line_num)
            if to_line_num:
                content['toLineNum'] = to_line_num
            if log_content:
                content['isEnd'] = False
            body['code'] = 200
            body['msg'] = 'SUCCESS'
            body['content'] = content
            client_send(connection, body)


def other(connection, msg):
    body = {
        'code': 500,
        'msg': msg
    }
    client_send(connection, body)


while True:
    print("监听端口%d..." % PORT)
    client_connection, client_address = listen_socket.accept()
    request_line, request_headers, request_body = RequestParser.parse_request_message(client_connection)
    print("请求行:%s" % request_line)
    print("请求头:%s" % request_headers)
    print("请求体:%s" % request_body)

    http_method = RequestParser.get_http_method(request_line)
    print("本次请求方法:%s" % http_method)
    if 'POST' != http_method:
        other(client_connection, 'invalid request, HttpMethod not support.')

    http_uri = RequestParser.get_http_uri(request_line).strip(' ')
    print("本次请求URI:%s" % http_uri)
    if not http_uri:
        other(client_connection, 'invalid request, uri-mapping empty.')

    if '/beat' == http_uri:
        beat(client_connection)
    elif '/idleBeat' == http_uri:
        idle_beat(client_connection, request_body)
    elif '/run' == http_uri:
        run(client_connection, request_body)
    elif '/kill' == http_uri:
        kill(client_connection, request_body)
    elif '/log' == http_uri:
        log(client_connection, request_body)
    else:
        other(client_connection, 'uri not found.')



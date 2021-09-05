# -*- coding: utf-8 -*-

"""
@author: ha er
@software: PyCharm
@file: request_parser.py
@time: 2021/8/31 9:00
"""
__author__ = 'ha er'


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

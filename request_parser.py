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
    _buf_size = 1024
    _line_mark = "\r\n"
    _content_length = "content-length"

    def parse_request_message(client_socket):
        request_line = None
        request_headers = {}
        request_body = None
        recv_data = ''
        while True:
            recv_data_buf = client_socket.recv(RequestParser._buf_size)
            if recv_data_buf == b'':
                return request_line, request_headers, request_body
            try:
                recv_data_buf = recv_data_buf.decode()
            except Exception as e:
                recv_data_buf = recv_data_buf.decode('gbk')

            recv_data += recv_data_buf
            # 判断是否有空行，有空行说明recv_data包含请求行和请求头，不包含则进行下次读取
            if RequestParser._blank_line in recv_data:
                break

        blank_line_index = recv_data.index(RequestParser._blank_line)
        request_base_data = recv_data[:blank_line_index]
        for index, line_data in enumerate(request_base_data.split(RequestParser._line_mark)):
            if index == 0:
                request_line = line_data
            else:
                key = line_data.split(':')[0]
                value = line_data.lstrip(key).lstrip(':')
                key = key.strip(' ').lower()
                value = value.strip(' ')
                request_headers[key] = value
        # 如果不包含content-length请求头，则没有请求体
        if RequestParser._content_length not in request_headers.keys():
            return request_line, request_headers, request_body

        content_length = int(request_headers[RequestParser._content_length])
        current_body = recv_data[blank_line_index + 4:]
        to_get_body_length = content_length - len(current_body)
        while to_get_body_length != 0:
            recv_data_buf = client_socket.recv(RequestParser._buf_size)
            try:
                recv_data_buf = recv_data_buf.decode()
            except Exception as e:
                recv_data_buf = recv_data_buf.decode('gbk')
            current_body += recv_data_buf
            to_get_body_length = to_get_body_length - len(recv_data_buf)

        request_body = current_body
        return request_line, request_headers, request_body







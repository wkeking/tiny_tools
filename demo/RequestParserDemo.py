# -*- coding: utf-8 -*-

"""
@author: ha er
@software: PyCharm
@file: RequestParserDemo.py
@time: 2021/8/31 10:30
"""
__author__ = 'ha er'
from request_parser import RequestParser
import socket


HOST = '0.0.0.0'
PORT = 8888
listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listen_socket.bind((HOST, PORT))
listen_socket.listen(1)
print('Serving HTTP on port %s ...' % PORT)

while True:
    client_connection, client_address = listen_socket.accept()
    request_line, request_headers, request_body = RequestParser.parse_request_message(client_connection)
    print(request_line)
    print(request_headers)
    print(request_body)
    http_response = """
HTTP/1.1 200 OK

Hello, World!
        """
    client_connection.sendall(http_response.encode("utf-8"))
    client_connection.close()
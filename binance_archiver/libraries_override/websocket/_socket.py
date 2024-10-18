import errno
import os.path
import selectors
import socket
from typing import Union

from ._exceptions import (
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
)
from ._ssl_compat import SSLError, SSLWantReadError, SSLWantWriteError
from ._utils import extract_error_code, extract_err_message

import time
import logging
from logging.handlers import RotatingFileHandler

def setup_logger(should_dump_logs: bool | None = False) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logging.Formatter.converter = time.gmtime

    console_formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03dZ %(levelname)s -- %(message)s",
        datefmt='%Y-%m-%dT%H:%M:%S'
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(console_formatter)

    logger.addHandler(stream_handler)

    if not os.path.exists('logs/'):
        os.makedirs('logs/')

    if should_dump_logs is True:
        file_handler = RotatingFileHandler(
            filename=f"logs/_socket.log",
            maxBytes=1 * 1024 * 1024,
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setFormatter(console_formatter)

        logger.addHandler(file_handler)

    return logger

temporary_logger = setup_logger(should_dump_logs=True)

"""
_socket.py
websocket - WebSocket client library for Python

Copyright 2024 engn33r

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

DEFAULT_SOCKET_OPTION = [(socket.SOL_TCP, socket.TCP_NODELAY, 1)]
if hasattr(socket, "SO_KEEPALIVE"):
    DEFAULT_SOCKET_OPTION.append((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
if hasattr(socket, "TCP_KEEPIDLE"):
    DEFAULT_SOCKET_OPTION.append((socket.SOL_TCP, socket.TCP_KEEPIDLE, 30))
if hasattr(socket, "TCP_KEEPINTVL"):
    DEFAULT_SOCKET_OPTION.append((socket.SOL_TCP, socket.TCP_KEEPINTVL, 10))
if hasattr(socket, "TCP_KEEPCNT"):
    DEFAULT_SOCKET_OPTION.append((socket.SOL_TCP, socket.TCP_KEEPCNT, 3))

_default_timeout = None

__all__ = [
    "DEFAULT_SOCKET_OPTION",
    "sock_opt",
    "setdefaulttimeout",
    "getdefaulttimeout",
    "recv",
    "recv_line",
    "send",
]


class sock_opt:
    def __init__(self, sockopt: list, sslopt: dict) -> None:
        if sockopt is None:
            sockopt = []
        if sslopt is None:
            sslopt = {}
        self.sockopt = sockopt
        self.sslopt = sslopt
        self.timeout = None


def setdefaulttimeout(timeout: Union[int, float, None]) -> None:
    """
    Set the global timeout setting to connect.

    Parameters
    ----------
    timeout: int or float
        default socket timeout time (in seconds)
    """
    global _default_timeout
    _default_timeout = timeout


def getdefaulttimeout() -> Union[int, float, None]:
    """
    Get default timeout

    Returns
    ----------
    _default_timeout: int or float
        Return the global timeout setting (in seconds) to connect.
    """
    return _default_timeout


def recv(sock: socket.socket, bufsize: int) -> bytes:
    if not sock:
        raise WebSocketConnectionClosedException("socket is already closed.")

    def _recv():
        try:
            return sock.recv(bufsize)
        except SSLWantReadError:
            pass
        except socket.error as exc:
            error_code = extract_error_code(exc)
            if error_code not in [errno.EAGAIN, errno.EWOULDBLOCK]:
                raise

        sel = selectors.DefaultSelector()
        sel.register(sock, selectors.EVENT_READ)

        r = sel.select(sock.gettimeout())
        sel.close()

        if r:
            return sock.recv(bufsize)

    try:
        if sock.gettimeout() == 0:
            bytes_ = sock.recv(bufsize)
        else:
            bytes_ = _recv()
    except TimeoutError:
        raise WebSocketTimeoutException("Connection timed out")
    except socket.timeout as e:
        message = extract_err_message(e)
        raise WebSocketTimeoutException(message)
    except SSLError as e:
        message = extract_err_message(e)
        if isinstance(message, str) and "timed out" in message:
            raise WebSocketTimeoutException(message)
        else:
            raise

    # temporary_logger.info('_socket test')

    if bytes_ == b'':
        print(f'print: received empty bytes_: {bytes_}')

        temporary_logger.info(f'Received empty bytes_: >>> {bytes_} <<< but no worries! Im continuing my work')
        return b''
        # raise WebSocketConnectionClosedException("Connection to remote host was lost.")
    else:
        return bytes_


def recv_line(sock: socket.socket) -> bytes:
    line = []
    while True:
        c = recv(sock, 1)
        line.append(c)
        if c == b"\n":
            break
    return b"".join(line)


def send(sock: socket.socket, data: Union[bytes, str]) -> int:
    if isinstance(data, str):
        data = data.encode("utf-8")

    if not sock:
        raise WebSocketConnectionClosedException("socket is already closed.")

    def _send():
        try:
            return sock.send(data)
        except SSLWantWriteError:
            pass
        except socket.error as exc:
            error_code = extract_error_code(exc)
            if error_code is None:
                raise
            if error_code not in [errno.EAGAIN, errno.EWOULDBLOCK]:
                raise

        sel = selectors.DefaultSelector()
        sel.register(sock, selectors.EVENT_WRITE)

        w = sel.select(sock.gettimeout())
        sel.close()

        if w:
            return sock.send(data)

    try:
        if sock.gettimeout() == 0:
            return sock.send(data)
        else:
            return _send()
    except socket.timeout as e:
        message = extract_err_message(e)
        raise WebSocketTimeoutException(message)
    except Exception as e:
        message = extract_err_message(e)
        if isinstance(message, str) and "timed out" in message:
            raise WebSocketTimeoutException(message)
        else:
            raise

import math
import requests
import time
from threading import Thread
from typing import Optional

from mapadroid.utils.CustomTypes import MessageTyping
from mapadroid.utils.logging import LoggerEnums, get_logger, get_origin_logger
from mapadroid.utils.madGlobals import (
    WebsocketWorkerConnectionClosedException, WebsocketWorkerRemovedException,
    WebsocketWorkerTimeoutException)
from mapadroid.worker.AbstractWorker import AbstractWorker


class WebsocketConnectedClientEntry:
    def __init__(self, origin: str, worker_thread: Optional[Thread], worker_instance: Optional[AbstractWorker],
                 rgc_communicator_url: str):
        self.origin: str = origin
        self.worker_thread: Optional[Thread] = worker_thread
        self.worker_instance: Optional[AbstractWorker] = worker_instance
        self.rgc_communicator_url = rgc_communicator_url
        self.logger = get_origin_logger(get_logger(LoggerEnums.websocket), origin=origin)
        self.errors = 0

    def send_and_wait(self, message: MessageTyping, timeout: float, worker_instance: AbstractWorker,
                      byte_command: Optional[int] = None) -> Optional[MessageTyping]:
        if isinstance(message, str):
            self.logger.debug("sending command: {}", message.strip())
            to_be_sent = message.encode('utf-8')
            ctype = 'text/plain'
        elif byte_command is not None:
            self.logger.debug("sending binary: {}", message[:10])
            to_be_sent: bytes = (int(byte_command)).to_bytes(4, byteorder='big')
            to_be_sent += message
            ctype = 'application/binary'
        else:
            self.logger.error("Tried to send invalid message (bytes without byte command or no byte/str passed)")
            return

        url = self.rgc_communicator_url + "/devices/" + self.origin + "/rgc"
        self.logger.debug('sending command to url %s' % url)
        headers = {'Content-Type': ctype}
        try:
            resp = requests.put(url, headers=headers, data=to_be_sent, timeout=timeout)
        except Exception as exc:
            self.logger.error('error waiting for RGC response from origin: %s' % exc)
            raise WebsocketWorkerTimeoutException

        if resp.status_code != 200:
            self.errors += 1
            self.logger.error('got non-200 response from RGC communcator: %d' % resp.status_code)
            # give RGC a chance to reconnect before saying it's gone.
            if resp.status_code == 404 and self.errors > 4:
                raise WebsocketWorkerConnectionClosedException
            raise WebsocketWorkerTimeoutException

        self.errors = 0

        ctype = resp.headers.get('Content-Type', None)
        if ctype is None:
            self.logger.error('RGC response has no content-type')
            raise WebsocketWorkerTimeoutException
        if ctype == 'text/plain':
            if isinstance(resp.content, bytes):
                resp = resp.content.decode('utf-8')
                self.logger.debug('got text response from RGC communicator: %s' % resp.strip())
                return resp
        self.logger.debug('got binary response from RGC communicator')
        return resp.content

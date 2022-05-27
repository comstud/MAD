import json
import logging
import queue
import requests
import requests.exceptions
import threading
import time
from typing import Coroutine, Dict, List, Optional, Set

from mapadroid.data_manager import DataManager
from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.mitm_receiver.MitmMapper import MitmMapper
from mapadroid.ocr.pogoWindows import PogoWindows
from mapadroid.utils.authHelper import check_auth
from mapadroid.utils.CustomTypes import MessageTyping
from mapadroid.utils.logging import (InterceptHandler, LoggerEnums, get_logger,
                                     get_origin_logger)
from mapadroid.utils.MappingManager import MappingManager
from mapadroid.websocket.AbstractCommunicator import AbstractCommunicator
from mapadroid.websocket.communicator import Communicator
from mapadroid.websocket.RGCClientInfo import WebsocketConnectedClientEntry
from mapadroid.websocket.RGCClientInfo import requests_session
from mapadroid.worker.AbstractWorker import AbstractWorker
from mapadroid.worker.WorkerFactory import WorkerFactory

logging.getLogger('websockets.server').setLevel(logging.DEBUG)
logging.getLogger('websockets.protocol').setLevel(logging.DEBUG)
logging.getLogger('websockets.server').addHandler(InterceptHandler(log_section=LoggerEnums.websocket))
logging.getLogger('websockets.protocol').addHandler(InterceptHandler(log_section=LoggerEnums.websocket))


logger = get_logger(LoggerEnums.websocket)

class WebsocketServer(object):
    def __init__(self, args, mitm_mapper: MitmMapper, db_wrapper: DbWrapper, mapping_manager: MappingManager,
                 pogo_window_manager: PogoWindows, data_manager: DataManager, event, enable_configmode: bool = False):
        self.__args = args
        self.__db_wrapper: DbWrapper = db_wrapper
        self.__mapping_manager: MappingManager = mapping_manager
        self.__pogo_window_manager: PogoWindows = pogo_window_manager
        self.__data_manager: DataManager = data_manager
        self.__mitm_mapper: MitmMapper = mitm_mapper
        self.__enable_configmode: bool = enable_configmode

        self._server_stopping = False

        self._rgc_polling_thread = threading.Thread(target=self._rgc_communicator_poller)

        self._current_devices_mutex = threading.Lock()
        self._current_devices: Dict[str, AbstractCommunicator] = {}
        self.__worker_factory: WorkerFactory = WorkerFactory(self.__args, self.__mapping_manager, self.__mitm_mapper,
                                                             self.__db_wrapper, self.__pogo_window_manager, event)

    def _handle_new_devices(self, devices):
        use_configmode = self.__enable_configmode
        for origin in devices:
            origin_logger = get_origin_logger(logger, origin=origin)
            use_cm = use_configmode
            device = None
            for _, dev in self.__data_manager.search('device', params={'origin': origin}).items():
                if dev['origin'] == origin:
                    device = dev
                    break
            if device is None:
                origin_logger.warning('Ignoring connection from unknown device')
                continue
            if not use_cm:
                if not self.__data_manager.is_device_active(device.identifier):
                    origin_logger.warning('Origin is currently paused. Unpause through MADmin to begin working')
                    use_cm = True
            communicator = self._get_communicator(origin, use_configmode=use_cm)
            if communicator is None:
                origin_logger.warning('no communicator created... invalid worker configured?')
                continue
            self._current_devices[origin] = communicator
            origin_logger.info('Found new RGC connection.. going to start worker...')
            communicator.websocket_client_entry.worker_thread.start()

    def _rgc_communicator_poller(self):
        url = self.__args.rgc_communicator_url + "/devices"
        poll_interval = 5
        next_poll = 0
        while not self._server_stopping:
            if time.time() < next_poll:
                time.sleep(1)
                continue
            logger.debug('Polling for connected RGC devices..')
            try:
                resp = requests_session.get(url, timeout=30)
                if resp.status_code != 200:
                    raise Exception('got status code %d' % resp.status_code)
                resp = json.loads(resp.content)
                devices = set([device['name'] for device in resp['devices']])
                with self._current_devices_mutex:
                    # check for dead workers first
                    rm_these = []
                    for device, communicator in self._current_devices.items():
                        if not communicator.websocket_client_entry.worker_thread.is_alive():
                            rm_these.append(device)
                    for device in rm_these:
                        origin_logger = get_origin_logger(logger, origin=device)
                        origin_logger.info('forgetting RGC connection because of dead worker')
                        del self._current_devices[device]
                    cur_devices = set(self._current_devices.keys())
                    new_devices = devices - cur_devices
                    if new_devices:
                        self._handle_new_devices(new_devices)
                    # we ignore gone devices, as communicator will return errors
                    # to the worker.. and the worker will shut down. We catch those
                    # above.
            except requests.exceptions.ConnectionError as exc:
                logger.error('Polling for connected RGC devices failed: RGC communicator not running')
            except Exception as exc:
                logger.exception('Polling for connected RGC devices failed: %s' % exc)

            next_poll = time.time() + poll_interval

    def start_server(self) -> None:
        logger.info("Starting websocket-server...")
        logger.debug("Device mappings: {}", self.__mapping_manager.get_all_devicemappings())

        self._rgc_polling_thread.start()

        while not self._server_stopping:
            time.sleep(1)

    def stop_server(self) -> None:
        logger.info('old WebsocketServer shutting down...')
        logger.info('Stopping RGC poller...')
        self._server_stopping = True
        self._rgc_polling_thread.join()
        logger.info("Stopped RGC poller, waiting for workers to die...")
        for device, communicator in self._current_devices.items():
            entry = communicator.websocket_client_entry
            origin_logger = get_origin_logger(logger, origin=device)
            origin_logger.info('Waiting for worker to stop')
            entry.worker_instance.stop_worker()
            entry.worker_thread.join()
            origin_logger.info('Worker has stopped')
        logger.info('old WebsocketServer is stopped.')

    def _get_communicator(self, origin, use_configmode: bool = None) -> Optional[WebsocketConnectedClientEntry]:
        entry = WebsocketConnectedClientEntry(origin=origin,
                                              worker_thread=None,
                                              worker_instance=None,
                                              rgc_communicator_url=self.__args.rgc_communicator_url)
        communicator: AbstractCommunicator = Communicator(
            entry, origin, None, self.__args.websocket_command_timeout)
        use_configmode: bool = use_configmode if use_configmode is not None else self.__enable_configmode
        worker: Optional[AbstractWorker] = self.__worker_factory \
            .get_worker_using_settings(origin, use_configmode, communicator=communicator)
        if worker is None:
            return None
        entry.worker_instance = worker
        entry.worker_thread = threading.Thread(name=origin, target=worker.start_worker)
        return communicator

    def terminate(self, origin):
        with self._current_devices_mutex:
            try:
                del self._current_devices[origin]
            except KeyError:
                pass

    def get_reg_origins(self) -> List[str]:
        with self._current_devices_mutex:
            return self._current_devices.keys()

    def get_origin_communicator(self, origin: str) -> Optional[AbstractCommunicator]:
        with self._current_devices_mutex:
            return self._current_devices.get(origin, None)

    def set_geofix_sleeptime_worker(self, origin: str, sleeptime: int) -> bool:
        comm = self.get_origin_communicator(origin)
        if comm is None:
            return False
        entry = comm.websocket_client_entry
        return entry.worker_instance.set_geofix_sleeptime(sleeptime)

    def set_job_activated(self, origin) -> None:
        self.__mapping_manager.set_devicesetting_value_of(origin, 'job', True)

    def set_job_deactivated(self, origin) -> None:
        self.__mapping_manager.set_devicesetting_value_of(origin, 'job', False)

    def force_disconnect(self, origin) -> None:
        pass

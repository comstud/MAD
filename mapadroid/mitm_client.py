import collections
import requests

Location = collections.namedtuple('Location', ['lat', 'lng'])

class MITMCommunicationError(Exception):
    pass

class MITMOriginNotFound(Exception):
    pass

class MITMClient:
    def __init__(self, url):
        if url.endswith('/'):
            url = url[:-1]
        self._url = url
        self._session = requests.Session()

    def get_latest_info(self, origin=None, include_protos=False):
        if origin is None:
            origin = '_all'
            all = True
        else:
            all = False
        url = self._url + '/origin_info/%s/latest' % origin
        if include_protos:
            url += '?protos=true'
        try:
            resp = self._session.get(url)
            if resp.status_code == 404:
                raise MITMOriginNotFound("origin '%s' not found" % origin)
            if resp.status_code != 200:
                raise MITMCommunicationError('error getting latest origin_info: got status code %s' % resp.status_code)
            resp = resp.json()
        except (MITMCommunicationError, MITMOriginNotFound):
            raise
        except Exception as exc:
            raise MITMCommunicationError('error getting latest origin_info: %s' % exc)

        latest_info = resp.get('latest_info', None)
        if latest_info is not None:
            if all:
                li = {}
                for origin, info in latest_info.items():
                    li[origin] = OriginInfo(origin, info)
                return li
            return OriginInfo(origin, latest_info)
        return None

    def get_protos_to_process(self, num=20):
        url = self._url + '/proto_queue/protos?limit=%d' % num
        try:
            resp = self._session.get(url)
            if resp.status_code != 200:
                raise MITMCommunicationError('error getting latest origin_info: got status code %s' % resp.status_code)
            resp = resp.json()
        except MITMCommunicationError:
            raise
        except Exception as exc:
            raise MITMCommunicationError('error getting latest origin_info: %s' % exc)
        return resp.get('protos', [])

class OriginInfo:
    def __init__(self, origin, data):
        self._origin = origin
        self._protos = None
        self._location = None
        for k, v in data.items():
            setattr(self, '_' + k, v)
        if self._location is not None:
            self._location = Location(self._location['lat'], self._location['lng'])
        if self._protos is not None:
            protos = {}
            for k, v in self._protos.items():
                protos[int(k)] = Proto(v)
            self._protos = protos

    @property
    def origin(self):
        return self._origin

    @property
    def location(self):
        return self._location

    @property
    def mitm_received_ts(self):
        return self._mitm_timestamp

    @property
    def received_ts(self):
        return self._receiver_timestamp

    def get_latest_proto(self, proto_num):
        if self._protos is None:
            # XXX(comstud): raise?
            return None
        return self._protos.get(proto_num)

class Proto:
    def __init__(self, data):
        # should always have these, but..
        self._timestamp = 0
        self._payload = None
        for k, v in data.items():
            setattr(self, '_' + k, v)
        if self._lat == 0 and self._lng == 0:
            self._location = None
        else:
            self._location = Location(self._lat, self._lng)

    @property
    def origin(self):
        return self._origin

    @property
    def location(self):
        return self._location

    @property
    def payload(self):
        return self._payload

    @property
    def is_raw(self):
        return self._raw

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def proto_id(self):
        return self._type

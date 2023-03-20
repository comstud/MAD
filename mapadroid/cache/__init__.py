import os

from mapadroid.cache.noopcache import NoopCache
from mapadroid.utils.logging import LoggerEnums, get_logger

logger = get_logger(LoggerEnums.system)

CACHE_INFO = {}

def _get_redis_cache(args):
    try:
        import redis
        import redis.backoff
        import redis.retry

        retry = redis.retry.Retry(redis.backoff.ExponentialBackoff(10, 0.25), 10)
        cache = redis.Redis(host=args.cache_host, port=args.cache_port, db=args.cache_database, retry=retry)
        cache.ping()
        return cache
    except ImportError:
        logger.error("Cache enabled but redis dependency not installed. Continuing without cache")
        return None
    except redis.exceptions.ConnectionError:
        logger.error("Unable to connect to Redis server. Continuing without cache")
        return None
    except Exception:
        logger.error("Unknown error while enabling cache. Continuing without cache")
        return None

def _get_noop_cache(args, required=False):
    if required:
        raise Exception('redis is required but we tried to return a NoopCache')
    noop_cache = CACHE_INFO.get('noop')
    if noop_cache is None:
        noop_cache = NoopCache()
        CACHE_INFO['noop'] = noop_cache
    return noop_cache

def get_cache(args, required=False):
    if not args.enable_cache:
        return _get_noop_cache(args, required=required)

    pid = os.getpid()
    cache_key = '%s|%s|%s' % (args.cache_host, args.cache_port, args.cache_database)

    if cache_key in CACHE_INFO:
        cache_info = CACHE_INFO[cache_key]
    else:
        cache_info = {}
        CACHE_INFO[cache_key] = cache_info

    if cache_info.get('pid', 0) == pid:
        return cache_info['redis']

    cache_info['pid'] = 0
    cache_info['redis'] = None

    cache = _get_redis_cache(args)
    if cache is None:
        return _get_noop_cache(args, required=required)
    cache_info['pid'] = pid
    cache_info['redis'] = cache

    return cache

import os

from mapadroid.cache.noopcache import NoopCache
from mapadroid.utils.logging import LoggerEnums, get_logger

logger = get_logger(LoggerEnums.system)

cache_info = {}

def _redis_is_ok(redis):
    try:
        redis.ping()
        return True
    except Exception:
        return False

def _get_redis_cache(args):
    try:
        import redis
        cache = redis.Redis(host=args.cache_host, port=args.cache_port, db=args.cache_database, single_connection_client=True)
    except ImportError:
        logger.error("Cache enabled but redis dependency not installed. Continuing without cache")
        return None
    except redis.exceptions.ConnectionError:
        logger.error("Unable to connect to Redis server. Continuing without cache")
        return None
    except Exception:
        logger.error("Unknown error while enabling cache. Continuing without cache")
        return None

    return cache

def get_cache(args):
    if not args.enable_cache:
        noop_cache = cache_info.get('noop')
        if noop_cache is None:
            noop_cache = NoopCache()
            cache_info['noop'] = noop_cache
        return noop_cache

    pid = os.getpid()

    if cache_info.get('pid', 0) == pid:
        cache = cache_info['redis']
        if _redis_is_ok(cache):
            return cache

    cache_info['pid'] = 0
    cache_info['redis'] = None

    cache = _get_redis_cache(args)
    if cache is not None:
        cache_info['pid'] = pid
        cache_info['redis'] = cache

    return cache

import logging

from tornado.gen import coroutine
from toredis.client import ClientPool, Client
from toredis.nodes import RedisNodes as BasicRedisNodes
from toredis.commands_future import RedisCommandsFutureMixin


logger = logging.getLogger(__name__)


class RedisClient(RedisCommandsFutureMixin, Client):
    pass


class RedisPool(RedisCommandsFutureMixin, ClientPool):
    client_cls = RedisClient


class RedisNodes(BasicRedisNodes):
    pool_cls = RedisPool


@coroutine
def check_db(name, db, ioloop):
    try:
        ret = yield db.setnx('db_name', name)
        if not ret:
            old_name = yield db.get('db_name')
            old_name = old_name.decode('utf-8')
            if name != old_name:
                logger.error("DB name changed from '%s' to '%s'",
                                                    old_name, name)
                ioloop.stop()
    except Exception:
        logger.exception("DB request is failed")
        ioloop.stop()


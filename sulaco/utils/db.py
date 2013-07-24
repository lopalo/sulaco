import logging

from tornado.concurrent import return_future
from toredis.commands import RedisCommandsMixin
from toredis.client import ClientPool, Client
from toredis.nodes import RedisNodes as BasicRedisNodes


logger = logging.getLogger(__name__)

### classes with future mixin ###

class RedisCommandsFutureMixin(RedisCommandsMixin):

    for mname in dir(RedisCommandsMixin):
        meth = getattr(RedisCommandsMixin, mname)
        if mname.startswith('_') or not callable(meth):
            continue
        locals()[mname] = return_future(meth)


class RedisClient(RedisCommandsFutureMixin, Client):
    pass


class RedisPool(RedisCommandsFutureMixin, ClientPool):
    client_cls = RedisClient


class RedisNodes(BasicRedisNodes):
    pool_cls = RedisPool

### check of db ###

class WrongDBNameError(Exception):
    pass


def check_db(name, db):
    try:
        ret = yield db.setnx('db_name', name)
        if not ret:
            old_name = yield db.get('db_name')
            old_name = old_name.decode('utf-8')
            if name != old_name:
                logger.error("DB name changed from '%s' to '%s'",
                                                    old_name, name)
                raise WrongDBNameError()
    except Exception:
        logger.exception("DB request is failed")
        raise

# scripts container

class NotLoadedScript(Exception):
    pass


class RedisScript(object):
    """
    Lua to Python conversion
    false -> Nil -> None
    true -> 1 (type int)
    """

    def __init__(self, script):
        self.script = script
        self.name = None # set by metaclass

    def __get__(self, inst, owner):
        if inst is None:
            return self
        if self.name not in inst._script_hashes:
            raise NotLoadedScript(self.name)
        sha1 = inst._script_hashes[self.name]
        def func(*keys, args=tuple()):
            return inst.evalsha(sha1, keys, args)
        return func


class RedisScriptsMeta(type):

    def __init__(self, name, bases, dct):
        super().__init__(name, bases, dct)
        self._script_fields = []
        for name in dir(self):
            f = getattr(self, name)
            if not isinstance(f, RedisScript):
                continue
            assert f.name is None
            f.name = name
            self._script_fields.append(f)


class RedisScriptsContainer(object, metaclass=RedisScriptsMeta):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._script_hashes = {}

    def load_scripts(self):
        for f in self._script_fields:
            sha1 = yield self.script_load(f.script)
            self._script_hashes[f.name] = sha1



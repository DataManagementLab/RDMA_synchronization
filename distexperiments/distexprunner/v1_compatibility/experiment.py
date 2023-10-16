import itertools
import logging


import distexprunner


class Server(distexprunner.Server):
    @property
    def data(self):
        return self


Logfile = distexprunner.File

def Printer(**kwargs):
    if 'fmt' in kwargs:
        kwargs['fmt'] = kwargs['fmt'].replace('{line}', '%s')
    for x in ['rstrip', 'end']:
        if x in kwargs:
            del kwargs[x]
    return distexprunner.Console(**kwargs)


class Base:
    pass



class errors:
    class NoConnectionError(Exception):
        pass


class actions:
    class Restart(Exception):
        pass


class time:
    sleep = distexprunner.sleep


class Proxy:
    def __init__(self, cls):
        self.server_list = distexprunner.ServerList(*cls.SERVERS)
        self.cls = cls
        self.__name__ = cls.__name__

    def __call__(self, servers):
        def target(server):
            if not isinstance(server, Server):
                server = servers[server]
            if not hasattr(server, '_Server__client'): #connection has failed
                raise errors.NoConnectionError
            return server

        try:
            self.cls().experiment(target)
        except actions.Restart:
            return distexprunner.Action.RESTART


class factory:
    class Grid:
        def __init__(self, factory_fn, *args):
            for params in itertools.product(*args):
                cls = factory_fn(*params)

                if not issubclass(cls, Base):
                    raise Exception('Factory needs to return a child of experiment.Base')

                suffix = '_'.join(map(str, params))
                cls.__name__ += f'_{suffix}'
                logging.info(f'Generated experiment: {cls.__name__}')

                proxy = Proxy(cls)
                distexprunner.reg_exp(proxy.server_list)(proxy)

    class Generator:
        def __init__(self, factory_fn, generator):
            for params in generator:
                cls = factory_fn(*params)

                if not issubclass(cls, Base):
                    raise Exception('Factory needs to return a child of experiment.Base')

                suffix = '_'.join(map(str, params))
                cls.__name__ += f'_{suffix}'
                logging.info(f'Generated experiment: {cls.__name__}')
                
                proxy = Proxy(cls)
                distexprunner.reg_exp(proxy.server_list)(proxy)
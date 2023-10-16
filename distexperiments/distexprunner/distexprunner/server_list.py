import asyncio
import logging
import types
from collections.abc import Iterable

from .server import Server


class ServerList:
    def __init__(self, *args, working_directory=None):
        servers = []
        args = list(args)
        while args:
            arg = args.pop(0)
            if isinstance(arg, Server):
                servers.append(arg)
            elif isinstance(arg, Iterable):
                args[0:0] = arg     # insert at front to preserve order
            else:
                raise Exception(f'Unsupported Argument type: {type(arg)}')

        if len(set(s.id for s in servers)) != len(servers):
            raise Exception('Server IDs must be unique')

        self.__servers = servers
        self.__id_to_server = {s.id: s for s in servers}
        self.__loop = asyncio.get_event_loop()
        self.__working_directory = working_directory


    def cd(self, directory):
        for s in self.__servers:
            s.cd(directory)


    def _connect_to_all(self):
        if not self.__servers:
            return
        task = asyncio.wait([s._connect() for s in self.__servers])
        self.__loop.run_until_complete(task)
        self.cd(self.__working_directory)


    def _disconnect_from_all(self):
        if not self.__servers:
            return
        task = asyncio.wait([s._disconnect() for s in self.__servers])
        self.__loop.run_until_complete(task)


    def wait_cmds_finish(self):
        raise NotImplementedError()


    def __getitem__(self, key):
        if isinstance(key, int):
            try:
                return self.__servers[key]
            except IndexError:
                raise Exception(f'IndexError for: {key}')
        elif isinstance(key, str):
            try:
                return self.__id_to_server[key]
            except KeyError:
                raise Exception(f'KeyError for: {key}')
        elif isinstance(key, slice):
            return ServerList(self.__servers[key])
        elif isinstance(key, types.FunctionType):
            return ServerList(filter(key, self.__servers))
        elif isinstance(key, tuple):
            return ServerList(self.__getitem__(k) for k in key)
        else:
            raise Exception(f'Lookup type: {type(key)} not supported')
        
    def __iter__(self):
        return iter(self.__servers)

    def __len__(self):
        return len(self.__servers)
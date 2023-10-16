import asyncio
import logging
import sys

from ._exceptions import BadReturnCode
from ._server_interface import ServerInterface
from ._client_interface import ClientInterface
from ._rpc import RPCReader, RPCWriter



class ClientImpl(ClientInterface):
    def __init__(self, reader, writer):
        self.__rpc_reader = RPCReader(reader, writer, self)
        self.rpc = RPCWriter(ServerInterface)(writer)

        self.pings = 0
        self.rc_futures = {}
        self.stdout_handler = {}
        self.stderr_handler = {}
    
    async def _on_disconnect(self):
        pass
        # logging.info(f'pings={self.pings}')

    
    # TODO refactor out
    async def _run_cmd(self, uuid, cmd, env):
        loop = asyncio.get_running_loop()
        rc_future = loop.create_future()
        
        self.rc_futures[uuid] = rc_future
        await self.rpc.run_cmd(uuid, cmd, env=env)

        return rc_future

    def _set_stdout(self, uuid, handler):
        self.stdout_handler[uuid] = handler

    def _set_stderr(self, uuid, handler):
        self.stderr_handler[uuid] = handler
            
    
    async def pong(self, *args, **kwargs):
        # await asyncio.sleep(0.1)
        self.pings += 1
        await self.rpc.ping(*args, **kwargs)


    async def stdout(self, uuid, line):
        for handler in self.stdout_handler.get(uuid, []):
            handler(line)

    async def stderr(self, uuid, line):
        for handler in self.stderr_handler.get(uuid, []):
            handler(line)

    async def rc(self, uuid, rc):
        if uuid in self.rc_futures:
            if not self.rc_futures[uuid].done():
                self.rc_futures[uuid].set_result(rc)
        logging.info(f'uuid={uuid} finished with exit code: {rc}')
        if rc != 0:
            raise BadReturnCode(rc)
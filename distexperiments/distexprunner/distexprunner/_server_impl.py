import asyncio
import logging
import shlex
import os
import sys
import signal
import subprocess
import atexit

from ._server_interface import ServerInterface
from ._client_interface import ClientInterface
from ._rpc import RPCReader, RPCWriter



class ServerImpl(ServerInterface):
    def __init__(self, reader, writer):
        self.__rpc_reader = RPCReader(reader, writer, self)
        self.rpc = RPCWriter(ClientInterface)(writer)

        self.pings = 0
        self.__processes = {}
        self.__cwd = None

        # TODO kill processes if client doesn't respond to ping

        self.__stdbuf_so = subprocess.check_output(
            "stdbuf -oL env | awk -F'=' '/^LD_PRELOAD=/ {print $2}'",
            shell=True,
            encoding='utf-8'
        ).strip()

        atexit.register(self.__at_exit)
    
    async def _on_disconnect(self):
        for uuid in self.__processes.keys():
            await self.kill_cmd(uuid)

        atexit.unregister(self.__at_exit)


    def __at_exit(self):
        num_procs = len(self.__processes)

        for uuid, p in self.__processes.items():
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                logging.info(f'killed: {uuid} {p.pid}')
            except ProcessLookupError:
                #logging.info(f'could not find uuid={uuid} with pid={p.pid}')
                pass

        logging.info(f'Killed {num_procs} running process')
          
    
    async def ping(self, *args, **kwargs):
        # await asyncio.sleep(0.1)
        self.pings += 1
        await self.rpc.pong(*args, **kwargs)

    
    async def cd(self, directory):
        self.__cwd = directory

    
    async def run_cmd(self, uuid, cmd, env={}):
        logging.info(f'uuid={uuid} cmd={cmd}')

        async def _read_stream(stream, rpc):
            while True:
                line = await stream.readline()
                if not line:
                    break
                sys.stdout.write(line.decode('utf-8'))
                await rpc(uuid, line.decode('utf-8'))
                
        
        environ = os.environ.copy()
        environ.update({k: str(v) for k, v in env.items()})
        environ['_STDBUF_O'] = 'L'
        environ['LD_PRELOAD'] = f'{environ.get("LD_PRELOAD", "")}:{self.__stdbuf_so}'
     

        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            env=environ,
            cwd=self.__cwd,
            start_new_session=True
        )
        self.__processes[uuid] = process
        logging.info(f'Attach gdb: gdb -p {process.pid}')

        await asyncio.wait([
            _read_stream(process.stdout, self.rpc.stdout),
            _read_stream(process.stderr, self.rpc.stderr)
        ])
        rc = await process.wait()
        logging.info(f'Got rc={rc} for: {repr(cmd)}')
        await self.rpc.rc(uuid, rc)

    
    async def __process_startup(self, uuid):
        waits = 0
        while uuid not in self.__processes:
            await asyncio.sleep(0.1)
            waits += 1
            if waits == 10:
                logging.error(f'{uuid} did never startup')
                return

    async def kill_cmd(self, uuid):
        await self.__process_startup(uuid)

        try:
            os.killpg(os.getpgid(self.__processes[uuid].pid), signal.SIGKILL)
            logging.info(f'killed: {uuid} {self.__processes[uuid].pid}')
        except ProcessLookupError:  #two kills
            #logging.info(f'could not find uuid={uuid} with pid={self.__processes[uuid].pid}')
            pass
            # TODO maybe send error to client

    
    async def stdin_cmd(self, uuid, line, close=False):
        await self.__process_startup(uuid)
        p = self.__processes[uuid]

        if p.stdin.is_closing():
            logging.error(f'{uuid} has stdin closed')
            return

        sys.stdout.write(line)
        sys.stdout.flush()

        p.stdin.write(line.encode())
        if close:
            p.stdin.write_eof()

        try:
            await p.stdin.drain()
        except ConnectionResetError:
            logging.error(f'ConnectionResetError')
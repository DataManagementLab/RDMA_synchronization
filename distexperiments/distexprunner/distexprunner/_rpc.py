import asyncio
import logging
import json

try:
    from asyncio.exceptions import IncompleteReadError
except ModuleNotFoundError: # for python3.7
    from asyncio.streams import IncompleteReadError


def RPCWriter(RPCInterface):
    class Writer(RPCInterface):
        def __init__(self, writer):
            self._writer = writer

        def __getattribute__(self, attr):
            writer = super().__getattribute__('_writer')

            async def func(*args, **kwargs):
                data = {'method': attr, 'args': args, 'kwargs': kwargs}
                logging.debug(f'call: {data}')
                writer.write(f'{json.dumps(data)}\n'.encode())
                try:
                    await writer.drain()
                except ConnectionResetError:
                    return False
                return True
            
            return func
    
    return Writer


class RPCReader:
    def __init__(self, reader, writer, impl):
        self.__reader = reader
        self.__writer = writer 
        self.__impl = impl

        addr = writer.get_extra_info('peername')
        logging.info(f'Initiated connection with {addr[0]}:{addr[1]}')

        loop = asyncio.get_running_loop()
        loop.create_task(self._read_loop())

    
    async def _read_loop(self):
        while True:
            if self.__writer.is_closing():
                break

            try:
                data = await self.__reader.readuntil(separator=b'\n')
            except (IncompleteReadError, ConnectionResetError):
                break

            json_data = json.loads(data[:-1])
            logging.debug(f'json: {json_data}')

            func = getattr(self.__impl, json_data['method'])
            # await func(*json_data['args'], **json_data['kwargs']) 
            asyncio.create_task(func(*json_data['args'], **json_data['kwargs']) )

        self.__writer.close()
        try:
            await self.__writer.wait_closed()
        except ConnectionResetError:
            pass

        addr = self.__writer.get_extra_info('peername')
        logging.info(f'Lost connection with {addr[0]}:{addr[1]}')
        await self.__impl._on_disconnect()

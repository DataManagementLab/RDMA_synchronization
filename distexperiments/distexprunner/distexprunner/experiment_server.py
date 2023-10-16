import asyncio
import logging
from contextlib import suppress

from ._server_impl import ServerImpl


class ExperimentServer:
    def __init__(self, ip, port, max_idle):
        self.ip = ip
        self.port = port
        self.loop = asyncio.get_event_loop()
        
        if max_idle > 0:
            self.start_terminator(max_idle)


    def start(self):
        self.stop_future = self.loop.create_future()
        self.loop.create_task(self.listen())

        try:
            self.loop.run_until_complete(self.stop_future)
        except KeyboardInterrupt:
            pass
    
        logging.info('Closing server')
        tasks = asyncio.all_tasks(loop=self.loop)
        for task in tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                self.loop.run_until_complete(task)
        logging.info(f'Cancelled {len(tasks)} running tasks.')
        self.loop.close()


    def start_terminator(self, max_idle):
        async def checker():
            idle_time_left = max_idle
            while idle_time_left > 0:
                await asyncio.sleep(1)
                if len(asyncio.all_tasks(loop=self.loop)) > 2:  # listen() and checker()
                    idle_time_left = max_idle
                else:
                    idle_time_left -= 1
            logging.info(f'Auto termination after being {max_idle} seconds idle.')
            self.stop_future.set_result(None)

        self.loop.create_task(checker())


    async def listen(self):
        server = await asyncio.start_server(ServerImpl, self.ip, self.port)
        addr = server.sockets[0].getsockname()
        logging.info(f'Serving on {addr[0]}:{addr[1]}')

        async with server:
            await server.serve_forever()


  
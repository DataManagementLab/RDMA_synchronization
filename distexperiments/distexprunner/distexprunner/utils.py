import asyncio
import sys, tty, termios
import logging
import itertools
import functools


__all__ = ['GDB', 'SOCKET_BIND', 'sleep', 'forward_stdin_to', 'counter', 'log', 'IterClassGen', 'any_failed']


GDB = f'gdb -quiet --ex run --args'
SOCKET_BIND = lambda nodes: f'numactl --cpunodebind={nodes} --membind={nodes}'


def sleep(delay):
    """sequential sleep without blocking event loop processing"""

    loop = asyncio.get_event_loop()
    async def task():
        await asyncio.sleep(delay)
    loop.run_until_complete(task())


def forward_stdin_to(cmd, esc='\x1b'): # \x02 ESC \x03 Ctrl-C
    """forward console stdin to running command (A BIT BUGGY)"""

    logging.info(f'Interfacing with command in progress... Press ESC to quit.')

    loop = asyncio.get_event_loop()
    async def task():
        future = loop.create_future()

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setraw(fd)

        def on_stdin():
            c = sys.stdin.read(1)
            # print(repr(c))

            if c == '\r':
                c = '\n'

            cmd.async_stdin(c)

            if c == esc:
                future.set_result(None)

        loop.add_reader(fd, on_stdin)
        await future
        loop.remove_reader(fd)

        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    loop.run_until_complete(task())
    

def counter(start=0, step=1):
    """Helper to count, c = counter(0); next(c)==0 next(c)==1"""
    return itertools.count(start=start, step=step)


LOG_LEVEL_CMD = 99
logging.addLevelName(LOG_LEVEL_CMD, 'LOG')

def log(message):
    """Log message using logging system with tag LOG"""
    logging.log(LOG_LEVEL_CMD, f'{message}')


class IterClassGen:
    """Generates and stores conveniently as many instances of classes as needed"""
    def __init__(self, cls, *args, **kwargs):
        self.__factory = functools.partial(cls, *args, **kwargs)
        self.__instances = []

    def __next__(self):
        instance = self.__factory()
        self.__instances.append(instance)
        return instance

    
    def __getitem__(self, key):
        return self.__instances[key]
        
    def __iter__(self):
        return iter(self.__instances)

    def __len__(self):
        return len(self.__instances)



def any_failed(cmds, poll_interval=1):
    """Checks periodically return-codes of commands, returns first rc found to be != 0, else False"""
    while True:
        rcs = [cmd.wait(block=False) for cmd in cmds]
        for rc in rcs:
            if rc is not None and rc != 0:
                return rc
        if all(rc == 0 for rc in rcs):
            return False
        sleep(poll_interval)
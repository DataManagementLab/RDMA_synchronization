import sys
import tty
import termios
import logging
import functools
import asyncio



class StdinController:
    def __init__(self):
        self.cmds = {}

        LOG_LEVEL_CMD = 98
        logging.addLevelName(LOG_LEVEL_CMD, 'CONTROL')
        self.__log = functools.partial(logging.log, LOG_LEVEL_CMD)
        self.__loop = asyncio.get_event_loop()
        self.__stop_future = self.__loop.create_future()
        self.__input = ''


    def add(self, server_id, _uuid, cmd, rpc):
        self.__log(f'Added cmd={cmd} uuid={_uuid} to controller')
        self.cmds[_uuid] = (server_id, cmd, rpc)


    def __menu(self, select=None):
        self.__cmd = None
        if select is None:
            sys.stdout.write('\n\r')
            self.__log(f'Interfacing with commands in progress...')
            self.__log('')
            self.__log(f'Press Ctrl-C to quit this controller.')
            self.__log(f'Press Ctrl-D to close stdin of active command.')
            self.__log(f'Press Ctrl-H to print this selection menu.')
            self.__log('')
            self.__log(f'Please select command from the list.')
            for i, (uuid, (server_id, cmd, _)) in enumerate(self.cmds.items()):
                self.__log(f'\t[{i}] {server_id}: {repr(cmd)}')
                self.__log(f'\t{" "*(len(server_id)+len(str(i))+5)}uuid={uuid}')

            sys.stdout.write('Enter selection: ')
            sys.stdout.flush()
            return
        
        try:
            index = int(select)
        except ValueError:
            sys.stdout.write('\n\r')
            self.__log(f'Number could not be converted to int: {select}')
            sys.stdout.write('Enter selection: ')
            sys.stdout.flush()
            return

        if index < 0 or index >= len(self.cmds):
            sys.stdout.write('\n\r')
            self.__log(f'Number out of range: {index}')
            sys.stdout.write('Enter selection: ')
            sys.stdout.flush()
            return

        self.__cmd = list(self.cmds)[index]
        server_id, cmd, _ = self.cmds[self.__cmd]
        sys.stdout.write('\n\r')
        self.__log(f'Opening to stdin of cmd={repr(cmd)} on {server_id}')



    def __on_stdin(self):
        c = sys.stdin.read(1)

        if c == '\x03':     # Ctrl-C
            self.__stop_future.set_result(None)
        elif c == '\x08': # Ctrl-H:
            self.__menu()
        elif c == '\x04':   # Ctrl-D
            if self.__cmd is not None:
                _, _, rpc = self.cmds[self.__cmd]
                self.__loop.create_task(rpc.stdin_cmd(self.__cmd, '', close=True))
                self.__stop_future.set_result(None)
                self.__input = ''
        elif c == '\x7f':   # Backspace
            self.__input = self.__input[:-1]
            sys.stdout.write(f'\b\033[K')
        elif c == '\r':     # Enter
            if self.__cmd is not None:
                _, _, rpc = self.cmds[self.__cmd]
                self.__loop.create_task(rpc.stdin_cmd(self.__cmd, f'{self.__input}\n', close=False))
                self.__input = ''
                sys.stdout.write('\r\n')
            else:
                self.__menu(select=self.__input)
                self.__input = ''
        else:
            self.__input += c
            # print(repr(c))
            sys.stdout.write(c)

        sys.stdout.flush()


    def wait(self):
        self.__menu()

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        tty.setraw(fd)

        self.__loop.add_reader(fd, self.__on_stdin)
        self.__loop.run_until_complete(self.__stop_future)
        self.__loop.remove_reader(fd)

        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        sys.stdout.write('\r\n')
        self.__log(f'Controller exited. Continuing execution...')
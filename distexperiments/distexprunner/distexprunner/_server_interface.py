
class ServerInterface:
    async def ping(self, x):
        raise NotImplementedError()

    async def run_cmd(self, uuid, cmd, env={}):
        raise NotImplementedError()

    async def kill_cmd(self, uuid):
        raise NotImplementedError()

    async def stdin_cmd(self, uuid, line, close=False):
        raise NotImplementedError()

    async def cd(self, directory):
        raise NotImplementedError
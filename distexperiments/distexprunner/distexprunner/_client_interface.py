
class ClientInterface:
    async def pong(self, x):
        raise NotImplementedError()

    async def stdout(self, uuid, line):
        raise NotImplementedError()

    async def stderr(self, uuid, line):
        raise NotImplementedError()

    async def rc(self, uuid, rc):
        raise NotImplementedError()
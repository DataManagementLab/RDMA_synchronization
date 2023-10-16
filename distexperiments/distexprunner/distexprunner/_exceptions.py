class BadReturnCode(Exception):
    def __init__(self, rc):
        super().__init__(f'BadReturnCode: {rc}')
import itertools
import inspect
from collections.abc import Iterable


def product(params):
    # filter out empty params
    params = {k: v for k, v in params.items() if len(v) != 0}
    keys = list(params.keys())
    values = params.values()

    def fn(x, pool):
        if isinstance(pool, ComputedParam):
            pool = pool.get(keys[:len(x)], x)
        
        if len(pool) == 0:
            yield x

        for y in pool:
            yield x + [y]


    result = [[]]
    for pool in values:
        result = [y for x in result for y in fn(x, pool)]

    for prod in result:
        yield {key: prod[i] for i, key in enumerate(keys)}



class ParameterGrid:
    def __init__(self, **kwargs):
        self.__params = kwargs

    def get(self):
        for params in product(self.__params):
            yield params, '_'.join(f'{k}={v}' for k, v in params.items())
             


class ComputedParam:
    def __init__(self, fn):
        self.fn = fn
        self.fn_args = inspect.getfullargspec(fn).args


    def get(self, keys, values):
        args = {key: values[i] for i, key in enumerate(keys) if key in self.fn_args}
        ret = self.fn(**args)
        if not isinstance(ret, Iterable):
            return (ret, )
        return ret

    def __len__(self):
        return 1
    
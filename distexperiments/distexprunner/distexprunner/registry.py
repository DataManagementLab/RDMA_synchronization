import functools
import collections

from .server_list import ServerList
from .parameter_grid import ParameterGrid


class ExperimentStore:
    __experiments = []

    @staticmethod
    def get():
        return ExperimentStore.__experiments

    @staticmethod
    def add(*, name, servers, func, params, max_restarts, raise_on_rc):
        ExperimentStore.__experiments.append(
            (name, servers, func, params, max_restarts, raise_on_rc)
        )



def reg_exp(servers=None, params=None, max_restarts=0, raise_on_rc=True):
    if not isinstance(servers, ServerList):
        raise Exception('Servers needs to be a ServerList')

    if params:
        if not isinstance(params, ParameterGrid):
            raise Exception('params needs to be a ParameterGrid')

        def decorator_grid(func):
            for p, name in params.get():
                name = func.__name__+'__'+name

                ExperimentStore.add(
                    name=name,
                    servers=servers,
                    func=func,
                    params=p,
                    max_restarts=max_restarts,
                    raise_on_rc=raise_on_rc
                )
        return decorator_grid


    def decorator(func):
        ExperimentStore.add(
            name=func.__name__,
            servers=servers,
            func=func,
            params={},
            max_restarts=max_restarts,
            raise_on_rc=raise_on_rc
        )
    return decorator

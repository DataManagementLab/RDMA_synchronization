import logging
import pathlib
import json


class ResumeManager:
    def __init__(self):
        self.path = pathlib.Path('.distexprunner')
        self.already_run = set()
        if self.path.exists():
            with self.path.open('r') as f:
                self.already_run = set(l for l in f.read().splitlines() if len(l) > 0)
        



    def was_run(self, exp, params):
        s = json.dumps({
            'name': exp,
            'params': params
        }, sort_keys=True)
        return s in self.already_run


    def add_run(self, exp, params):
        s = json.dumps({
            'name': exp,
            'params': params
        }, sort_keys=True)
        with self.path.open('a+') as f:
            f.write(f'{s}\n')

    
    def reset(self):
        self.already_run = set()
        if self.path.exists():
            self.path.unlink()

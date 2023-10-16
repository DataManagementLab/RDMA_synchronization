import logging
import asyncio
import os
import copy
import re
from collections.abc import Iterable


LOG_LEVEL_CMD = 100
logging.addLevelName(LOG_LEVEL_CMD, 'CONSOLE')


class Console:
    def __init__(self, fmt='%s'):
        self.fmt = fmt

    def __call__(self, line):
        logging.log(LOG_LEVEL_CMD, self.fmt % line.rstrip())


class File:
    def __init__(self, filename, append=False, flush=False):
        mode = 'a' if append else 'w'
        self.file = open(filename, mode)
        self.flush = flush

    def __del__(self):
        self.file.close()

    def __call__(self, line):
        self.file.write(line)
        if self.flush:
            self.file.flush()


class SubstrMatcher:
    def __init__(self, substr):
        self.substr = substr
        self.reset()

    def __call__(self, line):
        if self.__future.done():
            return  # TODO maybe make resetable
        if self.substr in line:
            self.__future.set_result(None)

    def wait(self):
        self.__loop.run_until_complete(self.__future)
        return True

    def matched(self):
        return self.__future.done()

    def reset(self):
        self.__loop = asyncio.get_event_loop()
        self.__future = self.__loop.create_future()


class EnvParser:
    def __call__(self, line):
        name, value = line.split('=', 1)
        self.__dict__[name] = value.rstrip()

    def __getitem__(self, key):
        return self.__dict__[key]



class CSVGenerator:
    """
    Generates CSV files from stdout/stderr.
    Takes a list of regexes with named groups, outputs csv to file or .header/.row.
    Use in conjuction with IterClassGen:
    
    csvs = IterClassGen(CSVGenerator, [
        r'total_table_agents=(?P<total_table_agents>\d+)',
    ])
    s.run_cmd('bin', stdout=next(csvs)).wait()
    for csv in csvs:
        csv.write('file.csv')
    """
    class Default:
        def __init__(self, regex):
            self.regex = re.compile(regex)
            self._cols = {k: None for k in self.regex.groupindex.keys()}

        def keys(self):
            return self._cols.keys()

        def cols(self):
            for k, v in self._cols.items():
                if v is not None:
                    yield (k, v)

        def search(self, line):
            match = self.regex.search(line)
            if not match:
                return

            self._cols.update(match.groupdict())


    class Array:
        def __init__(self, regex):
            self.regex = re.compile(regex)
            self._cols = {k: [] for k in self.regex.groupindex.keys()}

        def keys(self):
            return self._cols.keys()

        def cols(self):
            for k, v in self._cols.items():
                yield (k, '|'.join(v))

        def search(self, line):
            match = self.regex.search(line)
            if not match:
                return

            for k, v in match.groupdict().items():
                self._cols[k].append(v)


    class Percentile(Array):
        def __init__(self, regex, percentile):
            super().__init__(regex)
            self._percentile = percentile
            if not 0 <= percentile <= 1:
                raise Exception('Percentile must be between 0 and 1')

        def cols(self):
            for k, v in self._cols.items():
                if len(v) > 0:
                    yield (k, str(sorted(map(eval, v))[int(len(v)*self._percentile)]))
                else:
                    yield (k, '0')


    class Mean(Array):
        def cols(self):
            for k, v in self._cols.items():
                if len(v) > 0:
                    yield (k, str(sum(map(eval, v))/float(len(v))))
                else:
                    yield (k, '0')

    class Max(Array):
        def cols(self):
            for k, v in self._cols.items():
                if len(v) > 0:
                    yield (k, str(max(map(eval, v))))
                else:
                    yield (k, '0')


    class Min(Array):
        def cols(self):
            for k, v in self._cols.items():
                if len(v) > 0:
                    yield (k, str(min(map(eval, v))))
                else:
                    yield (k, '0')


    class Sum(Array):
        def cols(self):
            for k, v in self._cols.items():
                yield (k, str(sum(map(eval, v))))


    class SortedArray:
        def __init__(self, regex):
            self.regex = re.compile(regex)
            keys = list(self.regex.groupindex.keys())
            try:
                keys.remove('i')
            except ValueError:
                raise Exception('Sorted array needs to have <i> field to be able to sort')
            self._cols = {k: [] for k in keys}

        def keys(self):
            return self._cols.keys()

        def cols(self):
            for k, v in self._cols.items():
                v.sort(key=lambda x: x[0])
                yield (k, '|'.join(map(lambda x: x[1], v)))

        def search(self, line):
            match = self.regex.search(line)
            if not match:
                return

            d = match.groupdict()
            idx = int(d.pop('i'))
            for k, v in d.items():
                self._cols[k].append((idx, v))


    class GroupedArray:
        def __init__(self, regex):
            self.regex = re.compile(regex)
            keys = list(self.regex.groupindex.keys())
            try:
                keys.remove('key')
            except ValueError:
                raise Exception('Grouped array needs to have <key> field to be able to group')
            self._cols = {k: [] for k in keys}

        def keys(self):
            return self._cols.keys()

        def cols(self):
            for k, v in self._cols.items():
                yield (k, '|'.join(map(lambda x: '~'.join(map(str, x)), v)))

        def search(self, line):
            match = self.regex.search(line)
            if not match:
                return

            d = match.groupdict()
            key = int(d.pop('key'))
            for k, v in d.items():
                self._cols[k].append((key, v))



    def __init__(self, *args, **kwargs):
        self.__header = []
        self.__regexs = []
        self.__manual = {}

        self.add_columns(**kwargs)

        def flatten(l):
            for el in l:
                if isinstance(el, Iterable) and not isinstance(el, (str, bytes)):
                    yield from flatten(el)
                else:
                    yield el
        regexs = list(flatten(args))

        for regex in copy.deepcopy(regexs):
            if isinstance(regex, str):
                regex = self.Default(regex)
            
            for key in regex.keys():
                if key in self.__header:
                    raise Exception(f'{key} already in header set {self.__header}')
                self.__header.append(key)

            self.__regexs.append(regex)


    def __call__(self, line):
        for regex in self.__regexs:
            regex.search(line)


    def add_columns(self, **kwargs):
        for key, val in kwargs.items():
            if key in self.__header:
                raise Exception(f'{key} already in header set {self.__header}')
            self.__header.append(key)
            self.__manual[key] = str(val)


    @property
    def header(self):
        return ','.join(self.__header)

    @property
    def row(self):
        columns = {k: v for regex in self.__regexs for k, v in regex.cols()}
        columns.update(self.__manual)

        if len(columns.keys()) != len(self.__header):
            diff = set(self.__header)-set(columns.keys())
            raise Exception(f'Not enough values for row:\n{diff}')
            
        return ','.join(map(lambda k: columns[k], self.__header))


    def write(self, file):
        write_header = not os.path.exists(file)
        with open(file, 'a+') as f:
            if write_header:
                f.write(f'{self.header}\n')
            f.write(f'{self.row}\n')
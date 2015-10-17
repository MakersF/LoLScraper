import gzip
import os
from json import JSONEncoder
import datetime

def __attributes_to_dict(object, fields):
    return {field:getattr(object, field) for field in fields}

def datetime_to_dict(dt):
    return __attributes_to_dict(dt, ('year', 'month', 'day', 'hour', 'minute', 'second'))

def deltatime_to_dict(dt):
    return __attributes_to_dict(dt, ('days', 'seconds'))

class JSONConfigEncoder(JSONEncoder):

    def default(self, o):
        if hasattr(o, '__iter__'):
            return [x for x in o]

        elif isinstance(o, datetime.datetime):
            return datetime_to_dict(o)

        elif isinstance(o, datetime.timedelta):
            return deltatime_to_dict(o)

        elif hasattr(o, 'to_json'):
            return o.to_json()

        return super().default(o)

class AutoSplittingFile:
    """
    This class can be used to store lines. Every matches_per_file lines it opens a new file to write to.
    """
    extension = ".json.gz"

    def __init__(self, dir_path, matches_per_file=0, prefix="", file_name_postfix =""):
        self._dir = dir_path
        self._prefix = prefix
        self._matches_per_file = matches_per_file
        self._file = None
        self._postfix = file_name_postfix
        self._stored_matches = 0
        self._index = 0

    def open(self, path):
        if self._file:
            self.close()
        self._file = gzip.open(path, 'wt')

    def generate_file_path(self):
        date = datetime.datetime.now().isoformat().replace(":","-")
        name = '_'.join([ field for field in [self._prefix, date, self._postfix, self.extension] if field])
        return os.path.realpath(os.path.join(self._dir, name))

    def close(self):
        if self._file:
            self._file.close()
            self._file = None
            self._stored_matches = 0

    def write(self, text):
        if self._matches_per_file and self._stored_matches >= self._matches_per_file:
            self.close()
        if not self._file:
            self.open(self.generate_file_path())
        elif self._stored_matches != 0:
            # the file is not new, so a line has been written before. Add a  new line
            self._file.write('\n')

        self._file.write(text)
        self._stored_matches += 1

class TierStore:

    """
    This class handles several stores in parallel.
    """
    def __init__(self, dir_path, lines_per_store=1000, file_name=""):
        self._stores = {}
        self._dir = dir_path
        self._file_name = file_name
        self._lines_per_store = lines_per_store

    def store(self, text, tier):
        """
        Writes text to the underlying Store mapped at tier. If the store doesn't exists, yet, it creates it
        :param text: the text to write
        :param tier: the tier used to identify the store
        :return:
        """
        store = self._stores.get(tier, None)
        if not store:
            store = AutoSplittingFile(self._dir, self._lines_per_store, self._file_name, tier)
            self._stores[tier] = store
        store.write(text)

    def close(self):
        for value in self._stores.values():
            value.close()
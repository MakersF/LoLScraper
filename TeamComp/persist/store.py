import datetime
import gzip
import os

class Store:
    extension = ".json.gz"

    def __init__(self, dir_path, matches_per_file=1000, prefix="", file_name_postfix =""):
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
        date = datetime.datetime.now().isoformat()
        name =  '{0}_{1}_{2}_{3}'.format(self._prefix, date, self._postfix, self.extension)
        return os.path.join(self._dir, name)

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

    def __init__(self, dir_path, lines_per_store=1000, file_name=""):
        self._stores = {}
        self._dir = dir_path
        self._file_name = file_name
        self._lines_per_store = lines_per_store

    def store(self, text, tier):
        store = self._stores.get(tier, None)
        if not store:
            store = Store(self._dir, self._lines_per_store, self._file_name, tier)
            self._stores[tier] = store
        store.write(text)

    def close(self):
        for value in self._stores.values():
            value.close()
import unittest
import tempfile
import gzip
from contextlib import closing
from persist.store import Store, TierStore

class StoreTest(unittest.TestCase):

    prefix = "test"
    postfix = "file"
    tmp_dir = tempfile.gettempdir()
    lines_per_file = 50

    def setUp(self):
        self.store = Store(self.tmp_dir, self.lines_per_file, self.prefix, self.postfix)

    def tearDown(self):
        self.store.close()

    def test_file_opening_closing(self):
        self.assertIsNone(self.store._file)
        name = self.store.generate_file_path()
        self.store.open(name)
        self.assertIsNotNone(self.store._file)
        self.assertFalse(self.store._file.closed)
        self.assertEqual(name, self.store._file.name)
        self.store.close()
        self.assertIsNone(self.store._file)

    def test_write(self):
        some_text = "test_write"
        self.store.write(some_text)
        file = self.store._file.name
        self.store.close()
        with gzip.open(file, 'rt') as f:
            self.assertEqual(some_text, f.readline().strip())

    def test_multiple_writes(self):
        some_text = "test_multiple_writes"
        times = min(10, self.lines_per_file -1)
        name = self.store.generate_file_path()
        self.store.open(name)
        for i in range(times):
            self.assertEqual(i, self.store._stored_matches)
            self.store.write(some_text)
            self.assertEqual(name, self.store._file.name)
            self.assertEqual(i+1, self.store._stored_matches)

        self.store.close()
        with gzip.open(name, 'rt') as f:
            i = 0
            for line in f.readlines():
                self.assertEqual(some_text, line.strip())
                i += 1
            self.assertEqual(times,i)

    def test_multi_file_writing(self):
        some_text = "test_multi_file_writing"
        self.store.open(self.store.generate_file_path())
        file = self.store._file.name
        written_lines = 55
        for i in range(self.lines_per_file):
            self.store.write(some_text)
            self.assertEqual(file, self.store._file.name)

        self.store.write(some_text)
        self.assertEqual(1, self.store._stored_matches)
        second_file= self.store._file.name
        for i in range(written_lines - self.lines_per_file -1):
            self.assertEqual(i+1, self.store._stored_matches)
            self.store.write(some_text)
            self.assertEqual(second_file, self.store._file.name)

        self.assertNotEqual(file, second_file)
        self.store.close()
        with gzip.open(file, 'rt') as f:
            i = 0
            for line in f.readlines():
                self.assertEqual(some_text, line.strip())
                i += 1
            self.assertEqual(self.lines_per_file, i)

        with gzip.open(second_file, 'rt') as f:
            i = 0
            for line in f.readlines():
                self.assertEqual(some_text, line.strip())
                i += 1
            self.assertEqual(written_lines-self.lines_per_file, i)

class TierStoreTest(unittest.TestCase):

    tmp_dir = tempfile.gettempdir()
    prefix = "test"
    lines_per_store = 10

    def setUp(self):
        self.ts = TierStore(self.tmp_dir, self.lines_per_store, self.prefix)

    def tearDown(self):
        pass

    def check_store_label(self, label):
        tier = self.ts._stores[label]
        self.assertIsNotNone(tier)
        self.assertTrue(label in tier._file.name)

    def check_write_line_store(self, label, n_lines, expected, text):
        for i in range(n_lines):
            self.ts.store(text + label, label)
        tier = self.ts._stores[label]
        self.assertEqual(expected, tier._stored_matches)

    def test_store_creation(self):
        text = "test_store_creation"
        label = "tier"
        self.ts.store(text, label)
        self.check_store_label(label)
        tier = self.ts._stores[label]
        self.assertEqual(1, tier._stored_matches)

    def test_multiple_stores(self):
        text = "test_multiple_stores "
        label1 = "test1"
        label2 = "test2"
        self.ts.store(text + label1, label1)
        self.ts.store(text + label2, label2)
        self.check_store_label(label1)
        self.check_store_label(label2)
        self.assertEqual(1, self.ts._stores[label1]._stored_matches)
        self.assertEqual(1, self.ts._stores[label2]._stored_matches)

        lines = self.lines_per_store -2
        self.check_write_line_store(label1, lines, lines+1, text)
        self.check_write_line_store(label2, lines, lines+1, text)

    def test_multiple_file_one_store(self):
        text = "test_multiple_file_one_store"
        label1 = "test_1"
        self.check_write_line_store(label1, 2*self.lines_per_store +5, 5, text)

    def test_multiple_file_multiple_store(self):
        text = "test_multiple_file_multiple_store"
        for i in range(5):
            self.check_write_line_store("label " + str(i), 2*self.lines_per_store +5, 5, text)

    def test_auto_closing(self):
        with closing(self.ts):
            self.test_multiple_file_multiple_store()

        for store in self.ts._stores.values():
            self.assertIsNone(store._file)

if __name__ == '__main__':
    unittest.main()

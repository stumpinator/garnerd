import unittest
from pathlib import Path
from src.garnerd.filestore.directory import DirectoryFileStore

class TestGarnerd(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    @classmethod
    def setUpClass(cls):
        return super().setUpClass()
    
    @classmethod
    def tearDownClass(cls):
        return super().tearDownClass()
    
    def testFileStore_file_path(self):
        fs = DirectoryFileStore('/', dir_depth=6)
        t: Path = fs.file_path(path_key='56bb3d0a2a7f294967f02dbc2de2a403ae3ba98b124d840273a6e46e081cf67c', file_size=123)
        self.assertEqual(str(t.name), '0a2a7f294967f02dbc2de2a403ae3ba98b124d840273a6e46e081cf67c.3r')
        self.assertEqual(str(t.parent), '/5/6/b/b/3/d')
        self.assertEqual(str(t), '/5/6/b/b/3/d/0a2a7f294967f02dbc2de2a403ae3ba98b124d840273a6e46e081cf67c.3r')
        
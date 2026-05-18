import unittest
import asyncio
from pathlib import Path
from src.garnerd.filestore.directory import DirectoryFileStore
from tempfile import TemporaryDirectory


class TestDirectoryFileStore(unittest.TestCase):
    
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
    
    def test_file_path(self):
        with TemporaryDirectory(dir='/dev/shm') as tdir:
            fs = DirectoryFileStore(tdir, dir_depth=6)
            t: Path = fs.file_path(path_key='56bb3d0a2a7f294967f02dbc2de2a403ae3ba98b124d840273a6e46e081cf67c', file_size=123)
            self.assertEqual(str(t.name), '0a2a7f294967f02dbc2de2a403ae3ba98b124d840273a6e46e081cf67c.3r')
            self.assertEqual(str(t.parent), f'{tdir}/5/6/b/b/3/d')
            self.assertEqual(str(t), f'{tdir}/5/6/b/b/3/d/0a2a7f294967f02dbc2de2a403ae3ba98b124d840273a6e46e081cf67c.3r')
    
    def test_enum_sub_dirs(self):
        with TemporaryDirectory(dir='/dev/shm') as tdir:
            dfs = DirectoryFileStore(path=tdir)
            subdirs = list(dfs.enum_sub_dirs())
        self.assertEqual(len(subdirs), len(dfs.hexchars) ** dfs.dir_depth)
        with TemporaryDirectory() as tdir:
            dfs = DirectoryFileStore(path=tdir)
            dfs.dir_depth = 3
            dfs.hexchars = ['a', 'b', 'c', 'd']
            subdirs = list(dfs.enum_sub_dirs())
        self.assertEqual(len(subdirs), 4 ** 3)
        
    def test_create_dirs(self):
        with TemporaryDirectory(dir='/dev/shm') as tdir:
            dfs = DirectoryFileStore(path=tdir)
            dfs.dir_depth = 2
            subdirs = list(dfs.enum_sub_dirs())
            created = dfs.create_dirs()
            recreated = dfs.create_dirs()
        self.assertEqual(len(subdirs), len(dfs.hexchars) ** 2)
        self.assertEqual(len(subdirs), created)
        self.assertEqual(recreated, 0)
        
        with TemporaryDirectory(dir='/dev/shm') as tdir:
            dfs = DirectoryFileStore(path=tdir)
            dfs.dir_depth = 2
            subdirs = list(dfs.enum_sub_dirs())
            created = asyncio.run(dfs.create_dirs_async())
            recreated = asyncio.run(dfs.create_dirs_async())
        self.assertEqual(len(subdirs), len(dfs.hexchars) ** 2)
        self.assertEqual(len(subdirs), created)
        self.assertEqual(recreated, 0)
        
    def test_init_store(self):
        with TemporaryDirectory(dir='/dev/shm') as tdir:
            dfs = DirectoryFileStore(path=tdir)
            dfs.dir_depth = 2
            dcount, stored = dfs.init_store()
        self.assertEqual(dcount, len(dfs.hexchars) ** 2)
        self.assertEqual(stored, 0)
        
        with TemporaryDirectory(dir='/dev/shm') as tdir:
            dfs = DirectoryFileStore(path=tdir)
            dfs.dir_depth = 2
            dcount, stored = asyncio.run(dfs.init_store_async())
        self.assertEqual(dcount, len(dfs.hexchars) ** 2)
        self.assertEqual(stored, 0)
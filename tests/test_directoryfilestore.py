import unittest
import asyncio
from pathlib import Path
from src.garnerd.filestore.directory import DirectoryFileStore
from tempfile import TemporaryDirectory
from shutil import rmtree, copy
from os.path import isdir
from time import sleep


class TestDirectoryFileStore(unittest.TestCase):
    _temp_dir: TemporaryDirectory
    temp_dir: str
    test_files_path: str
    
    def setUp(self):
        self._temp_dir = TemporaryDirectory(dir='/dev/shm', delete=False)
        self.temp_dir = self._temp_dir.name
    
    def tearDown(self):
        self._temp_dir.cleanup()
        self._temp_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.test_files_path = Path(__file__).parent / "files"
        return super().setUpClass()
    
    @classmethod
    def tearDownClass(cls):
        return super().tearDownClass()
    
    def clean_temp(self):
        if not isdir(self.temp_dir):
            raise NotADirectoryError(f"{self.temp_dir} is not a directory")
        temp_path = Path(self.temp_dir)
        for item in temp_path.iterdir():
            if item.is_dir():
                rmtree(item)
            else:
                item.unlink()
    
    def test_file_path(self):
        self.clean_temp()
        fs = DirectoryFileStore(path=self.temp_dir, dir_depth=6)
        t: Path = fs.file_path(path_key='56bb3d0a2a7f294967f02dbc2de2a403ae3ba98b124d840273a6e46e081cf67c', file_size=123)
        self.assertEqual(str(t.name), '0a2a7f294967f02dbc2de2a403ae3ba98b124d840273a6e46e081cf67c.3r')
        self.assertEqual(str(t.parent), f'{self.temp_dir}/5/6/b/b/3/d')
        self.assertEqual(str(t), f'{self.temp_dir}/5/6/b/b/3/d/0a2a7f294967f02dbc2de2a403ae3ba98b124d840273a6e46e081cf67c.3r')
    
    def test_enum_sub_dirs(self):
        self.clean_temp()
        dfs = DirectoryFileStore(path=self.temp_dir)
        subdirs = list(dfs.enum_sub_dirs())
        self.assertEqual(len(subdirs), len(dfs.hexchars) ** dfs.dir_depth)
        
        self.clean_temp()
        dfs = DirectoryFileStore(path=self.temp_dir)
        dfs.dir_depth = 3
        dfs.hexchars = ['a', 'b', 'c', 'd']
        subdirs = list(dfs.enum_sub_dirs())
        self.assertEqual(len(subdirs), 4 ** 3)
        
    def test_create_dirs(self):
        self.clean_temp()
        dfs = DirectoryFileStore(path=self.temp_dir)
        dfs.dir_depth = 2
        subdirs = list(dfs.enum_sub_dirs())
        created = dfs.create_dirs()
        recreated = dfs.create_dirs()
        self.assertEqual(len(subdirs), len(dfs.hexchars) ** 2)
        self.assertEqual(len(subdirs), created)
        self.assertEqual(recreated, 0)
        
        self.clean_temp()
        dfs = DirectoryFileStore(path=self.temp_dir)
        dfs.dir_depth = 2
        subdirs = list(dfs.enum_sub_dirs())
        created = asyncio.run(dfs.create_dirs_async())
        recreated = asyncio.run(dfs.create_dirs_async())
        self.assertEqual(len(subdirs), len(dfs.hexchars) ** 2)
        self.assertEqual(len(subdirs), created)
        self.assertEqual(recreated, 0)
        
    def test_init_store(self):
        self.clean_temp()
        dfs = DirectoryFileStore(path=self.temp_dir)
        dfs.dir_depth = 2
        dcount, stored = dfs.init_store()
        self.assertEqual(dcount, len(dfs.hexchars) ** 2)
        self.assertEqual(stored, 0)
        
        self.clean_temp()
        dfs = DirectoryFileStore(path=self.temp_dir)
        dfs.dir_depth = 2
        dcount, stored = asyncio.run(dfs.init_store_async())
        self.assertEqual(dcount, len(dfs.hexchars) ** 2)
        self.assertEqual(stored, 0)
        
    def test_add_remove_file(self):
        pkey = "9ff938883748a4d3cf9c09a05b1f0ec073645cc26cda89fe4f4baa532ece9ca0"
        fsize = 1
        test_file = Path(self.test_files_path) / "plain_text.txt"
        
        self.clean_temp()
        dfs = DirectoryFileStore(path=self.temp_dir, dir_depth=2)
        added_path = dfs.file_path(path_key=pkey, file_size=fsize)
        dfs.init_store()
        self.assertFalse(added_path.is_file())
        dfs.add_file(source_path=str(test_file), path_key=pkey, file_size=fsize)
        self.assertTrue(added_path.is_file())
        dfs.remove_file(path_key=pkey, file_size=fsize)
        self.assertFalse(added_path.is_file())
        asyncio.run(dfs.add_file_async(source_path=str(test_file), path_key=pkey, file_size=fsize))
        self.assertTrue(added_path.is_file())
        asyncio.run(dfs.remove_file_async(path_key=pkey, file_size=fsize))
        self.assertFalse(added_path.is_file())
    
    def test_path_list(self):
        self.clean_temp()
        dfs = DirectoryFileStore(path=self.temp_dir)
        dfs.dir_depth = 2
        self.assertEqual(dfs.path_list(path_key="abcd"), ["a","b","cd"])
        dfs.dir_depth = 3
        self.assertEqual(dfs.path_list(path_key="abcdef"), ["a","b","c","def"])
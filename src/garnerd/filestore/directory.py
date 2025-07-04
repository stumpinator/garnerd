from pathlib import Path
from typing import Callable, Iterator
from shutil import disk_usage
from filelock import FileLock
from os import walk

from ..exceptions import InvalidDirectoryError, InvalidFileError, InvalidFileSize, InvalidPath


def size_to_basex(size: int, base_text="0123456789abcdefghijklmnopqrstuv") -> str:
    base = len(base_text)
    if size < base:
        return base_text[size]
    return size_to_basex(size // base, base_text=base_text) + base_text[size % base]


class DirectoryFileStore:
    path: Path
    dir_depth: int
    max_files: int
    min_free: float
    min_free_bytes: int
    max_file_size: int
    size_to_string: Callable[[int], str]
    initialized: bool
    dir_mode: int
    file_mode: int
    _stored: int
    hexchars = ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']
    
    def __init__(
            self,
            path: Path|str,
            dir_depth: int = 4,
            max_files: int = 999999999,
            min_free: float = 20.0,
            max_file_size: int = (128 * 10124 * 1024 * 1024),
            size_to_string: Callable[[int], str] = size_to_basex
        ):
        """Class for storing files in a directory structure.

        Args:
            path (Path | str): base path to store the 
            dir_depth (int, optional): max depth of subdirectories to store files. Defaults to 4.
            max_files (int, optional): max files to be stored. Defaults to 999999999.
            min_free (float, optional): minimum percentage of free space before rejecting ingest.
                Defaults to 20.0.
            max_file_size (int, optional): max file size before rejecting ingest.
                Defaults to 128GB (128 * 10124 * 1024 * 1024).
            size_to_string (Callable[[int], str], optional): function to convert file size to a string.
                Used to encode the file size to a smaller string and used as an extension.
                Defaults to size_to_basex.
        """
        self.path = Path(path)
        self.dir_depth = dir_depth
        self.max_files = max_files
        self.max_file_size = max_file_size
        self.size_to_string = size_to_string
        self.initialized = False
        self.dir_mode = 740
        self.file_mode = 440
        self._stored = 0
        
        if min_free >= 0 and min_free < 100:
            self.min_free = min_free
        else:
            self.min_free = 5.0
        du = disk_usage(str(self.path))
        self.min_free_bytes = int((self.min_free / 100) * du.total)

    def has_file(self, path_key: str, file_size: int) -> bool:
        """check if a file exists in a the store

        Args:
            path_key (str): a unique identifier for a file. typicall a hash.
            file_size (int): the size in bytes of the file

        Returns:
            bool: True if file exists
        """
        fpath = self.file_path(path_key=path_key, file_size=file_size)
        return fpath.exists() and fpath.is_file()
    
    def file_path(self, path_key: str, file_size: int) -> Path:
        """generate a unique file path for this store

        Args:
            path_key (str): a unique identifier for a file. typicall a hash
            file_size (int): the size in bytes of the file

        Raises:
            ValueError: invalid arguments
            InvalidPath: path_key is not long enough to convert to file
            InvalidFileSize: the size_to_string function failed

        Returns:
            Path: full path to a unique file name
        """
        if not isinstance(file_size, int) or file_size < 0:
            raise ValueError("Invalid size: must be integer >= 0")
        try:
            int(path_key, 16)
        except ValueError:
            raise ValueError("path_key must be a hex string")
        
        path_key = path_key.lower()
        if len(path_key) <= self.dir_depth:
            raise InvalidPath(f"path_key must be a string with a length greater than {self.dir_depth}.")
            
        size_string = self.size_to_string(file_size)
        if len(size_string) < 1:
            raise InvalidFileSize(f"size_to_string returned invalid string")
        
        subdirs = '/'.join(path_key[a] for a in range(0,self.dir_depth))
        fname = f"{path_key[self.dir_depth:]}.{size_string}"
        return self.path / subdirs / fname
    
    def init_store(self) -> tuple[int,int]:
        """Creates all required store directories if needed and counts already stored files

        Returns:
            tuple[int,int]: directories created, file stored
        """
        dcount = self.create_dirs(self.path)
        self._stored = self.count_stored()
        return dcount,self._stored
    
    def enum_sub_dirs(self, base_dir: str|Path = None, depth: int = 1, max_depth: int = None) -> Iterator[Path]:
        """Enumerates all bottom level directories used by the store

        Args:
            base_dir (str | Path, optional): where to start. Defaults to None which will use the store path.
            depth (int, optional): recursion helper. incremented on recursive calls to prevent infinite recursion.
                Defaults to 1.
            max_depth (int, optional): max level depth. Defaults to None which will use the store's configured max depth.

        Raises:
            ValueError: depth or max_depth is not valid integer.

        Yields:
            Iterator[Path]: each bottom level path in the store. this will be (16 ** max_depth) items.
        """
        if not isinstance(depth, int) or depth < 1:
            raise ValueError("depth must be integer >= 1")
        max_depth = max_depth or self.dir_depth
        if not isinstance(max_depth, int) or max_depth < depth:
            raise ValueError("max_depth must be integer >= depth")
        
        pd = base_dir or self.path
        pd: Path = Path(pd)
        
        if depth > max_depth:
            yield pd
        else:
            for hd in self.hexchars:
                nd = pd / hd
                yield from self.enum_sub_dirs(base_dir=nd, depth=depth+1, max_depth=max_depth)
    
    def create_dirs(self) -> int:
        """creates all directories used to store files

        Returns:
            int: number of directories created. will be 0 if they already exist.
        """
        fdirs = self.enum_sub_dirs()
        created = 0
        for fdir in fdirs:
            if not fdir.exists():
                fdir.mkdir(modr=self.dir_mode, parents=True, exist_ok=True)
                created += 1
        return created
    
    @staticmethod
    def file_count(dir_path: str):
        """
        Counts the total number of files in a given directory and its subdirectories.

        Args:
            dir_path (str): The path to the directory to start counting from.

        Returns:
            int: The total number of files found.
        """
        file_count = 0
        for root, _, files in walk(dir_path):
            file_count += len(files)
        return file_count
    
    def count_stored(self) -> int:
        """total number of files stored using the configured storage scheme

        Returns:
            int: files in store
        """
        count = 0
        for d in self.enum_sub_dirs():
            count += sum(1 for x in d.glob('*') if not x.match('*.lock'))
        return count
    
    def add_file(self, source_path: str, path_key: str, file_size: int) -> bool:
        """Adds a file to the store

        Args:
            source_path (str): source file to add
            path_key (str): a unique identifier for a file. typicall a hash
            file_size (int): the size in bytes of the file

        Raises:
            InvalidFileError: source_path is an invalid file
            InvalidDirectoryError: the directory this file is to be moved to doesn't exist.
                This is most likely because the store was not initialized.

        Returns:
            bool: True if the file exists in the store regardless if this action added the file.
        """
        src = source_path or ''
        src = Path(src)
        if not src.exists() or not src.is_file():
            raise InvalidFileError(f"source path is not a valid file")
        
        dst = self.file_path(path_key=path_key, file_size=file_size)
        lock = FileLock(dst)
        with lock:
            if not dst.exists():
                if not dst.parent.exists():
                    raise InvalidDirectoryError(f"Parent directory {str(dst.parent)} does not exist.")
                src.rename(dst)
                dst.chmod(mode=self.file_mode)
                self._stored += 1
            else:
                src.unlink()
        return dst.exists()
    
    def remove_file(self, path_key: str, file_size: int) -> bool:
        """Remove a file from the store

        Args:
            path_key (str): a unique identifier for a file. typicall a hash
            file_size (int): the size in bytes of the file

        Returns:
            bool: True if the file does not exist in the store regardless if this action removed the file.
        """
        fpath = self.file_path(path_key=path_key, file_size=file_size)
        lock = FileLock(f"{fpath}.lock")
        with lock:
            if fpath.exists() and fpath.is_file():
                fpath.unlink()
                self._stored -= 1
        return not fpath.exists()

    def get_free(self) -> float:
        """
        Returns:
            float: percentage of free disk space in the filesystem used by the store
        """
        du = disk_usage(str(self.path))
        return (du.free / du.total) * 100
    
    def get_free_bytes(self) -> int:
        """
        Returns:
            int: number of free bytes in the filesystem used by the store
        """
        du = disk_usage(str(self.path))
        return du.free
    
    def files_stored(self) -> int:
        """
        Returns:
            int: number of files stored
        """
        return self._stored
    
    def can_store(self, file_size: int) -> bool:
        """Determine if a file can be stored

        Args:
            file_size (int): size of file to be added

        Raises:
            InvalidFileSize: passed size was not valid integer >= 0

        Returns:
            bool: True if the file can be added
        """
        if self.get_free_bytes() < self.min_free_bytes:
            return False
        if self.files_stored() >= self.max_files:
            return False
        if not isinstance(file_size, int) and int < 0:
            raise InvalidFileSize(f"size must be an integer of at least 0")
        return self.max_file_size >= file_size

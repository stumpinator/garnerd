from hashlib import md5, sha1, sha224, sha256, sha384, sha512
from _hashlib import HASH
from collections.abc import Iterable, Iterator, Buffer
from concurrent import futures
from typing import Callable, Optional, BinaryIO, Self
from multiprocessing import Manager

from ..buffers import SHMFanoutBuffer, SHMBufferSync
from ..buffers import SHMProcessor, FileReadingProcessor, FileMagicProcessor, FileWritingProcessor


class HashingConfig:
    _buff_size: int
    _selected_hashes: set[str]
    _config: dict[str,int|bool]
    
    def __init__(self, **kwargs):
        self._config = None
        # add default values not passed as keyword
        cfg = self.default_config | kwargs
        self._selected_hashes = set()
        for k in self.available():
            if cfg.get(k, False) == True:
                self._selected_hashes.add(k)
        self._buff_size = cfg['buff_size']
    
    def hashers(self) -> dict[str, HASH]:
        """Instances of all selected hasher objects

        Returns:
            dict[str, HASH]: key = hash_label, value = instance of HASH object
        """
        hashers = dict()
        sys_hashers = self.system_hashers()
        for k in self._selected_hashes:
            hashers[k] = sys_hashers[k]()
        return hashers
    
    @property
    def buff_size(self) -> int:
        """
        Returns:
            int: size of buffer used when reading bytes to hash
        """
        return self._buff_size
    
    @property
    def config(self) -> dict[str,int|bool]:
        """hashing configuration in dictionary form.
            This can be used in place of to_dict()

        Returns:
            dict[str,int|bool]: key/value par of all hashing configuration settings
        """
        if self._config is None:
            self._config = dict(buff_size=self._buff_size)
            for k in self.system_hashers():
                self._config[k] = k in self._selected_hashes
        # if config ever uses more complex objects, deepcopy will be required here
        return self._config.copy()
            
    def __repr__(self):
        return str(self.config)
    
    def available(self, hash_label: str|None = None) -> bool|set:
        """Determine if a specific hash is available for selection or return all available hashes

        Args:
            hash_label (str | None, optional): A label to check for. Defaults to None.

        Returns:
            bool|set: If hash_label was passed, returns True/False if hash is selectable.
                Otherwise, return frozenset of all selectable hashes.
        """
        sys_hasher = self.system_hashers()
        if hash_label is None:
            return set(sys_hasher.keys())
        else:
            return hash_label in sys_hasher
    
    def add_hash_type(self, hash_label: str) -> bool:
        """Add hash type to configuration. Must be a hash in .hashes()

        Args:
            hash_label (str): hash label to add

        Returns:
            bool: True if hash label is valid and added. Otherwise False.
        """
        if self.available(hash_label):
            self._selected_hashes.add(hash_label)
            self._config = None
            return True
        else:
            return False
    
    def selected(self, hash_label: str|None = None) -> bool|frozenset:
        """Determine if a specific hash is selected or return all selected hashes

        Args:
            hash_label (str | None, optional): A label to check for. Defaults to None.

        Returns:
            bool|frozenset: If hash_label was passed, returns True/False if hash is selected.
                Otherwise, return frozenset of all selected hashes
        """
        if hash_label is None:
            return frozenset(self._selected_hashes)
        else:
            return hash_label in self._selected_hashes
    
    def del_hash_type(self, hash_label: str) -> bool:
        """Removes hash type from configuration.

        Args:
            hash_label (str): hash to remove from selected

        Returns:
            bool: True if hash was selected. Otherwise False.
        """
        if self.selected(hash_label):
            self._selected_hashes.difference_update([hash_label])
            self._config = None
            return True
        else:
            return False
    
    @property
    def default_config(self) -> dict[str,int|bool]:
        """Default values for all configuration variables.

        Returns:
            dict: key/value pairs with default hasher class configuration
        """
        return dict(
                md5=True,
                sha1=True,
                sha256=True, 
                sha224=False, 
                sha384=False, 
                sha512=False, 
                buff_size=131072
            )

    @classmethod
    def system_hashers(cls) -> dict[str,Callable[[Optional[Buffer]],HASH]]:
        """All hashing provided by the system

        Returns:
            dict: key = hash name, value = class for instancing
        """
        return dict(md5=md5,
                    sha1=sha1,
                    sha256=sha256,
                    sha224=sha224,
                    sha384=sha384,
                    sha512=sha512)


class Hasher:
    """Hashes data using hashlib. Can perform multiple hashing ops per file.
    """
    _active_hashes: dict[str,HASH]
    _byte_count: int
    hashing_config: HashingConfig
    
    def __init__(self, **kwargs):
        """
            key (str) = hash label
            value (bool) = enable/disable that hasher
        """
        self.hashing_config = HashingConfig(**kwargs)
        self._active_hashes = self.hashing_config.hashers()
        self._byte_count = 0
        
    @property
    def active_hashes(self) -> dict[str,HASH]:
        """
        Returns:
            dict[str,HASH]: key = hash label, value = instanced hasher
        """
        return self._active_hashes
    
    def clear(self):
        """reset all hashing and byte counter
        """
        self.active_hashes = self.hashing_config.hashers()
        self._byte_count = 0
    
    def update(self, buffer: bytes|bytearray|memoryview):
        """updates all hashing objects
        """
        self._byte_count += len(buffer)
        for v in self.active_hashes.values():
            v.update(buffer)
    
    def report(self) -> dict[str,int|str]:
        """create a dict report of total bytes and hex digests
        """
        report = dict(size=self._byte_count)
        for k,v in self.active_hashes.items():
            report[k] = v.hexdigest()
        return report
    
    def hash_file(self, file_path: str) -> dict[str,int|str]:
        """Hashes and generates report using configured hash algorithms.
            call clear() first unless you want to start from a dirty state

        Args:
            file_path (str): file to hash

        Returns:
            dict: dictionary report of hash information / metadata for file
        """
        
        ba = bytearray(self.hashing_config.buff_size)
        mv = memoryview(ba)
        with open(file_path, 'rb', buffering=0) as f:
            while n := f.readinto(mv):
                self.update(mv[:n])
        
        report = self.report()            
        report['path'] = file_path
        return report
    
    @staticmethod
    def hash_file_worker(file_path: str, cfg: dict | None = None) -> dict:
        """Hashes file and returns report. Used by threading/multiprocessing as target.

        Args:
            file_path (str): file to hash
            cfg (dict): hasher configuration

        Returns:
            dict: hash report
        """
        report = dict(path=file_path, success=False, size=-1)
        if not isinstance(cfg, dict):
            cfg = {}
        hshr = Hasher(**cfg)
        
        try:
            hshr.hash_file(file_path=file_path)
        except Exception as e:
            report['exception'] = str(e)
            return report
        
        report.update(hshr.report())
        report['success'] = True
        return report

    def hash_multi(self, file_list: Iterable[str], max_threads: int = 2, cfg: dict | None = None)-> Iterator[dict]:
        """hash multiple files using concurrency
        
        Args:
            file_list (Iterable[str]): file paths to hash
            max_threads (int): number of available threads or process in the pool. default 2
            cfg (dict|None): Hasher config. If None (default) then use the Hasher class default config
            
        yields:
            Iterator[dict]: report for each file as completed
        """
        flist = list()
        with futures.ProcessPoolExecutor(max_workers=max_threads) as executor:
            for f in file_list:
                flist.append(executor.submit(self.hash_file_worker, f, cfg))
            for fr in futures.as_completed(flist):
                yield fr.result()


class HashingProcessor(SHMProcessor):
    hasher: HASH
    hash_label: str
    hexdigest: str

    def __init__(self, buff_syncs: Iterator[SHMBufferSync], hash_label: str = "md5"):
        """Used to perform simultaneous hashing with shared memory buffers.

        Args:
            buff_syncs (Iterator[SHMBufferSync]): shared memory buffer synch objects used to read data stream
            hash_label (str, optional): type of hash. Defaults to "md5".

        Raises:
            ValueError: invalid hash label
        """
        sys_hashers = HashingConfig.system_hashers()
        if hash_label not in sys_hashers:
            raise ValueError("hash_label must be an available system hasher")
        super().__init__(buff_syncs=buff_syncs)
        self.hasher = None
        self.hash_label = hash_label
        self.hexdigest = None
    
    def report(self):
        if self.hasher is not None:
            self.reporting[self.hash_label] = self.hasher.hexdigest()
        return super().report()
    
    def process(self):
        self.hasher = HashingConfig.system_hashers()[self.hash_label]()
        return super().process()
        
    def handle_data(self, data):
        self.hasher.update(data)


class SHMHasher:
    buffer_size: int
    buffers: list[SHMFanoutBuffer]
    
    def __init__(self, buffer_count: int = 2, buffer_size: int = (128 * 1024 * 1024)):
        """Use shared memory buffers to perform simultaneous hashing and other metadata processing for files

        Args:
            buffer_count (int, optional): number of shared memory buffers to use.
                Defaults to 2. More than 2 is probably not required and increasing probably won't improve performance.
            buffer_size (int, optional): size in bytes of each buffer. Defaults to 128M (128 * 1024 * 1024).
        """
        self.buffer_size = buffer_size
        buffer_count = max(buffer_count, 2)
        self.buffers = [SHMFanoutBuffer(create=True, size=self.buffer_size) for _ in range(0,buffer_count)]

    def hash_file(self, file_path: str, *args, file_magic: bool = True, file_mime: bool = True) -> dict:
        """hash a file

        Args:
            file_path (str): path to file
            file_magic (bool, optional): include file magic in report. Defaults to True.
            file_mime (bool, optional): include mime type in report. Defaults to True.

        Raises:
            Exception: Worker process errors

        Returns:
            dict: report containing metadata including any set hashes.
        """
        report = dict()
        with futures.ProcessPoolExecutor() as executor:
            processor_list: list[SHMProcessor] = list()
            futures_list = list()
            
            bsyncs = [SHMBufferSync(x.name, None) for x in self.buffers]
            filereader = FileReadingProcessor(bsyncs, file_path=file_path)
            if file_magic or file_mime:
                processor_list.append(FileMagicProcessor(bsyncs, magic = file_magic, mime = file_mime))
            for arg in args:
                try:
                    hp = HashingProcessor(bsyncs, hash_label=arg)
                except ValueError:
                    pass
                else:
                    processor_list.append(hp)
            max_threads = len(processor_list) + 1
            
            mgr = Manager()
            for bsync in bsyncs:
                bsync.barrier = mgr.Barrier(max_threads)
            
            reader = executor.submit(filereader.process)
            for proc in processor_list:
                futures_list.append(executor.submit(proc.process))
            
            report = reader.result()
            for fr in futures.as_completed(futures_list):
                d: dict = fr.result()
                if d is not None:
                    sz = d.get('size', 0)
                    if sz == report['size']:
                        report.update(d)
                    else:
                        raise Exception("A hash worker did not get expected number of bytes")
                else:
                    raise Exception("Worker process returned None")
            
        return report
    
    def __del__(self):
        for b in self.buffers:
            b.close()
            b.unlink()

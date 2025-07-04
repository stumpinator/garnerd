from typing import Callable, Optional, BinaryIO, Self
from multiprocessing.shared_memory import SharedMemory
from threading import Barrier
from struct import calcsize, pack_into, unpack_from



class SHMFanoutBuffer:
    """Uses a shared memory buffer for backing
    """
    _shmem: SharedMemory
    _max: int
    _created: bool
    _sz_struct: str
    _sz_width: int
    _unlinked: bool
    _closed: bool
    _size: int
    _mv: memoryview
    
    def __init__(
                self,
                size: int = (256 * 1024 * 1024), 
                name: str|None = None,
                create: bool = False
            ):
        self._sz_struct = "Q"
        self._sz_width = calcsize(self._sz_struct)
        min_size = size + self._sz_width
        self._shmem = SharedMemory(name=name, create=create, size=min_size)
        self._max = self._shmem.size - self._sz_width
        self._created = create
        self._unlinked = False
        self._closed = False
        self._size = 0
        self._mv = self._shmem.buf[self._sz_width:]
    
    @property
    def name(self):
        return self._shmem.name
    
    def save_size(self, new_size: int = None):
        """writes size to size struct at beginning of buffer
        """
        if new_size is not None:
            self._size = new_size
        pack_into(self._sz_struct, self._shmem.buf, 0, self._size)
        
    def load_size(self):
        """loads size from size struct at beginning of buffer
        """
        self._size = unpack_from(self._sz_struct, self._shmem.buf, 0)[0]
    
    def save_bytes(self, data) -> int:
        addlen = min(len(data), self._max)
        self._shmem.buf[self._sz_width:addlen] = data[:addlen]
        self._size = addlen
        self.save_size()
        return addlen
    
    def load_bytes(self) -> memoryview:
        self.load_size()
        return self.snapshot()
    
    def snapshot(self) -> memoryview:
        """
        Returns:
            memoryview: read only snapshot of memory contents
        """
        return self._shmem.buf[self._sz_width:self._size].toreadonly()
    
    def close(self):
        """calls close on the shared memory. required for all when no longer in use.
        """
        if not self._closed:
            self._mv.release()
            self._shmem.close()
            self._closed = True
            
    def unlink(self):
        """call unlink on the shared memory. required for creator.
        """
        if not self._unlinked and self._created:
            self._shmem.unlink()
            self._unlinked = True
        
    def __del__(self):
        self.close()
        self.unlink()
        if self._mv is not None:
            del self._mv
        if self._shmem is not None:
            del self._shmem

    @property
    def buf(self):
        return self._mv
    
    @property
    def full(self) -> bool:
        """
        Returns:
            bool: True if no more bytes can be added to buffer. otherwise False
        """
        return self._size >= self._max
    
    @property
    def size(self) -> int:
        """
        Returns:
            int: max bytes available in the buffer
        """
        return self._max
    
    def __len__(self) -> int:
        """
        Returns:
            int: bytes written to buffer
        """
        return self._size


class SHMBufferSync:
    shm_name: str
    barrier: Barrier|None
    _buffer: SHMFanoutBuffer|None
    timeout: float|None

    def __init__(self, shm_name: str, barrier: Barrier|None, timeout: float|None = None):
        self.shm_name = shm_name
        self.barrier = barrier
        self._buffer = None
        self.timeout = timeout
    
    def load_buffer(self):
        if self._buffer is None:
            self._buffer = SHMFanoutBuffer(size=0, name=self.shm_name, create=False)
    
    def close(self):
        if self._buffer is not None:
            self._buffer.close()
    
    @property
    def shmbuffer(self):
        return self._buffer
    
    def wait(self, timeout: float|None = None):
        timeout = timeout or self.timeout
        self.barrier.wait(timeout=self.timeout)
    
    def __hash__(self) -> int:
        return self.shm_name.__hash__()
    
    def __lt__(self, other: Self):
        return self.shm_name.__lt__(other.shm_name)
    
    def __del__(self):
        self.close()

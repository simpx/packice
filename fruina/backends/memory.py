import os
import tempfile
import mmap
import time
import uuid
from typing import Any, Optional
from ..core.blob import Blob, BlobView
from ..core.lease import Lease, AccessType
from ..core.object import Object

class MemBlob(Blob):
    def __init__(self, name: str):
        self.name = name
        self.fd = None
        self.file = None
        self.is_sealed = False
        
        if hasattr(os, 'memfd_create'):
            self.fd = os.memfd_create(name, os.MFD_CLOEXEC)
            self.file = open(self.fd, "wb+", buffering=0)
        else:
            self.file = tempfile.TemporaryFile(prefix=f"fruina_{name}_", mode="w+b")
            self.fd = self.file.fileno()

    def write(self, data: bytes) -> int:
        if self.is_sealed:
            raise ValueError("Blob is sealed")
        return self.file.write(data)

    def read(self, size: int = -1, offset: int = 0) -> bytes:
        self.file.seek(offset)
        return self.file.read(size)

    def truncate(self, size: int) -> None:
        if self.is_sealed:
            raise ValueError("Blob is sealed")
        self.file.truncate(size)

    def memoryview(self, mode: str = "rb") -> memoryview:
        prot = mmap.PROT_READ
        if 'w' in mode or '+' in mode:
            prot |= mmap.PROT_WRITE
        try:
            mm = mmap.mmap(self.fd, 0, prot=prot)
            return memoryview(mm)
        except ValueError:
            if os.fstat(self.fd).st_size == 0:
                return memoryview(b"")
            raise

    def seal(self) -> None:
        if self.is_sealed:
            return
        self.file.flush()
        self.is_sealed = True

    def get_handle(self) -> Any:
        return self.fd

    def close(self) -> None:
        if self.file:
            self.file.close()

    def delete(self) -> None:
        self.close()

class MemoryBlobView(BlobView):
    """
    Client-side view of a MemoryBlob.
    Wraps a file descriptor received from the server to access shared memory.
    """
    def __init__(self, fd: int, mode: str = "rb"):
        self.fd = fd
        self.mode = mode
        self._mmap = None
        self._buffer = None

    def write(self, data: bytes) -> int:
        return os.write(self.fd, data)

    def read(self, size: int = -1, offset: int = 0) -> bytes:
        if offset != -1:
            os.lseek(self.fd, offset, os.SEEK_SET)
        return os.read(self.fd, size)

    def truncate(self, size: int) -> None:
        os.ftruncate(self.fd, size)
        self._close_mmap()

    def memoryview(self, mode: str = "rb") -> memoryview:
        if self._buffer:
            return self._buffer

        try:
            size = os.fstat(self.fd).st_size
        except OSError:
            size = 0
            
        if size == 0:
            return memoryview(b"")

        prot = mmap.PROT_READ
        if 'w' in self.mode or '+' in self.mode:
            prot |= mmap.PROT_WRITE
        
        flags = mmap.MAP_SHARED
        
        try:
            self._mmap = mmap.mmap(self.fd, 0, flags=flags, prot=prot)
            self._buffer = memoryview(self._mmap)
            return self._buffer
        except Exception as e:
            raise ValueError(f"Failed to mmap: {e}")

    def seal(self) -> None:
        self._close_mmap()

    def get_handle(self) -> Any:
        return self.fd

    def close(self) -> None:
        self._close_mmap()
        if self.fd is not None:
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None

    def delete(self) -> None:
        self.close()

    def _close_mmap(self):
        if self._buffer:
            self._buffer.release()
            self._buffer = None
        if self._mmap:
            self._mmap.close()
            self._mmap = None

class MemoryLease(Lease):
    def __init__(self, object_id: str, access: AccessType, ttl: Optional[float] = None):
        self._lease_id = str(uuid.uuid4())
        self._object_id = object_id
        self._access = access
        self._ttl = ttl
        self.created_at = time.time()
        self.last_renewed_at = self.created_at
        self.is_active_flag = True

    @property
    def lease_id(self) -> str:
        return self._lease_id

    @property
    def object_id(self) -> str:
        return self._object_id

    @property
    def access(self) -> AccessType:
        return self._access

    @property
    def ttl(self) -> Optional[float]:
        return self._ttl

    def is_expired(self) -> bool:
        if not self.is_active_flag:
            return True
        if self._ttl is None:
            return False
        return (time.time() - self.last_renewed_at) > self._ttl

    def renew(self) -> None:
        if self.is_active_flag:
            self.last_renewed_at = time.time()

    def release(self) -> None:
        self.is_active_flag = False

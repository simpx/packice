import os
import mmap
from typing import Any
from ..core.blob import Blob, BlobView

class FileBlob(Blob):
    def __init__(self, path: str):
        self.path = path
        self.file = None
        self.is_sealed = False
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.file = open(path, "wb+")

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
        self.file.flush()

    def memoryview(self, mode: str = "rb") -> memoryview:
        prot = mmap.PROT_READ
        if 'w' in mode or '+' in mode:
            prot |= mmap.PROT_WRITE
        
        try:
            mm = mmap.mmap(self.file.fileno(), 0, prot=prot)
            return memoryview(mm)
        except ValueError:
            if os.fstat(self.file.fileno()).st_size == 0:
                return memoryview(b"")
            raise

    def seal(self) -> None:
        if self.is_sealed:
            return
        self.file.flush()
        self.file.close()
        self.file = open(self.path, "rb")
        self.is_sealed = True

    def get_handle(self) -> Any:
        return self.path

    def close(self) -> None:
        if self.file:
            self.file.close()

    def delete(self) -> None:
        self.close()
        if os.path.exists(self.path):
            try:
                os.remove(self.path)
            except OSError:
                pass

class FileBlobView(BlobView):
    """
    Client-side view of a FileBlob.
    Wraps a file path received from the server to access the file.
    """
    def __init__(self, path: str, mode: str = "rb"):
        self.path = path
        self.mode = mode
        self.fd = None
        self._mmap = None
        self._buffer = None
        
        flags = os.O_RDONLY
        if 'w' in mode or '+' in mode or 'a' in mode:
            flags = os.O_RDWR
        
        self.fd = os.open(path, flags)

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

import os
import mmap
from typing import Any
from ..core.blob import Blob

class FileBlob(Blob):
    def __init__(self, path: str):
        self.path = path
        self.file = None
        self.is_sealed = False
        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Open for writing initially
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
        # Re-open in read-only mode
        self.file = open(self.path, "rb")
        self.is_sealed = True

    def get_handle(self) -> Any:
        # For FS blob, the handle is the path
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

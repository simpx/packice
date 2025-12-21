import os
import tempfile
import mmap
from typing import Any
from ..core.blob import Blob

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
            # Fallback for non-Linux (e.g. macOS)
            # Create a temporary file that is deleted on close, but we keep it open
            self.file = tempfile.TemporaryFile(prefix=f"packice_{name}_")
            self.fd = self.file.fileno()

    def write(self, data: bytes) -> int:
        if self.is_sealed:
            raise ValueError("Blob is sealed")
        return self.file.write(data)

    def read(self, size: int = -1, offset: int = 0) -> bytes:
        self.file.seek(offset)
        return self.file.read(size)

    def seal(self) -> None:
        if self.is_sealed:
            return
        self.file.flush()
        # For memfd/tempfile, we don't necessarily need to close and reopen,
        # but we should enforce read-only logic in the wrapper.
        # We keep the FD open.
        self.is_sealed = True

    def get_handle(self) -> Any:
        # Return the file descriptor
        return self.fd

    def close(self) -> None:
        if self.file:
            self.file.close()

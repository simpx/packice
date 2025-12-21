import os
import tempfile
import mmap
import time
import uuid
from typing import Any, Optional
from ..core.blob import Blob
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

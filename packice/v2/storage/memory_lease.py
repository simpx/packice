import time
import uuid
from typing import Optional, Any
from ..core.lease import Lease, AccessType
from ..core.object import Object

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

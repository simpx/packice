import uuid
import time
from typing import Dict, Optional, Callable, Any, Tuple
from .object import Object, ObjectState
from .lease import Lease, AccessType
from .blob import Blob

BlobFactory = Callable[[str], Blob]  # object_id -> Blob
LeaseFactory = Callable[[str, AccessType, Optional[float]], Lease] # object_id, access, ttl -> Lease

class Peer:
    def __init__(self, blob_factory: Optional[BlobFactory] = None, lease_factory: Optional[LeaseFactory] = None):
        self._blob_factory = blob_factory
        self._lease_factory = lease_factory
        self.objects: Dict[str, Object] = {}
        self.leases: Dict[str, Lease] = {}

    def create_blob(self, object_id: str) -> Blob:
        """Creates a new Blob for the given object_id.
        Subclasses can override this to provide custom storage logic.
        """
        if self._blob_factory:
            return self._blob_factory(object_id)
        raise NotImplementedError("Peer subclasses must implement create_blob or provide a blob_factory")

    def create_lease(self, object_id: str, access: AccessType, ttl: Optional[float]) -> Lease:
        """Creates a new Lease.
        Subclasses can override this to provide custom lease logic.
        """
        if self._lease_factory:
            return self._lease_factory(object_id, access, ttl)
        raise NotImplementedError("Peer subclasses must implement create_lease or provide a lease_factory")

    def acquire(self, object_id: Optional[str], access: AccessType, ttl: Optional[float] = None, meta: Optional[Dict[str, Any]] = None) -> Tuple[Lease, Object]:
        self._cleanup_expired_leases()

        if object_id is None:
            if access in (AccessType.READ, AccessType.WRITE):
                raise ValueError(f"Cannot acquire {access.value} lease without object_id")
            object_id = str(uuid.uuid4())

        obj = self.objects.get(object_id)

        if access == AccessType.CREATE:
            if obj is not None:
                raise ValueError(f"Object {object_id} already exists")
            
            blob = self.create_blob(object_id)
            obj = Object(object_id, [blob], meta)
            self.objects[object_id] = obj
        
        elif access == AccessType.READ:
            if obj is None:
                raise KeyError(f"Object {object_id} not found")
            if not obj.is_sealed():
                raise ValueError(f"Object {object_id} is not sealed yet")

        elif access == AccessType.WRITE:
            if obj is None:
                raise KeyError(f"Object {object_id} not found")

        lease = self.create_lease(object_id, access, ttl)
        self.leases[lease.lease_id] = lease
        
        return lease, obj

    def seal(self, lease_id: str):
        lease = self._get_active_lease(lease_id)
        if lease.access != AccessType.CREATE:
            raise ValueError("Cannot seal a read lease")
        
        obj = self.objects.get(lease.object_id)
        if obj is None:
             raise KeyError(f"Object {lease.object_id} not found for lease {lease_id}")

        obj.seal()

    def discard(self, lease_id: str):
        """
        Permanently deletes the object associated with the lease.
        Requires a CREATE or WRITE lease.
        """
        lease = self._get_active_lease(lease_id)
        if lease.access not in (AccessType.CREATE, AccessType.WRITE):
            raise ValueError("Cannot discard with a read lease")
        
        object_id = lease.object_id
        obj = self.objects.get(object_id)
        
        if obj:
            obj.delete()
            del self.objects[object_id]
        
        self.release(lease_id)

    def release(self, lease_id: str):
        if lease_id not in self.leases:
            return
        lease = self.leases[lease_id]
        lease.release()
        del self.leases[lease_id]

    def _get_active_lease(self, lease_id: str, raise_error=True) -> Lease:
        lease = self.leases.get(lease_id)
        if lease and lease.is_expired():
            self.release(lease_id)
            lease = None
        
        if lease is None:
            if raise_error:
                raise KeyError(f"Lease {lease_id} not found or expired")
            return None
        return lease

    def _cleanup_expired_leases(self):
        expired = [lid for lid, l in self.leases.items() if l.is_expired()]
        for lid in expired:
            self.release(lid)

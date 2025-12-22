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
        # Check for expiration of existing leases first (lazy cleanup)
        self._cleanup_expired_leases()

        if object_id is None:
            if access == AccessType.READ:
                raise ValueError("Cannot acquire read lease without object_id")
            object_id = str(uuid.uuid4())

        obj = self.objects.get(object_id)

        if access == AccessType.CREATE:
            if obj is not None:
                # If object exists, we can't create it again unless it's a "restart" or we handle it.
                # For simplicity, if it exists, fail.
                raise ValueError(f"Object {object_id} already exists")
            
            # Create new object
            # For CREATE, we create the first blob
            blob = self.create_blob(object_id)
            obj = Object(object_id, [blob], meta)
            self.objects[object_id] = obj
        
        elif access == AccessType.READ:
            if obj is None:
                raise KeyError(f"Object {object_id} not found")
            if not obj.is_sealed():
                # Can we read while creating? 
                # v0 design says: "Objects are writable only while in CREATING... SEALED objects are immutable."
                # Usually read is allowed on SEALED.
                # If it's CREATING, maybe we can't read yet?
                # v0: "For read intent, may fail if the node lacks a sealed copy"
                raise ValueError(f"Object {object_id} is not sealed yet")

        lease = self.create_lease(object_id, access, ttl)
        self.leases[lease.lease_id] = lease
        
        # Return lease and the object
        return lease, obj

    def seal(self, lease_id: str):
        lease = self._get_active_lease(lease_id)
        if lease.access != AccessType.CREATE:
            raise ValueError("Cannot seal a read lease")
        
        obj = self.objects.get(lease.object_id)
        if obj is None:
             raise KeyError(f"Object {lease.object_id} not found for lease {lease_id}")

        obj.seal()
        # In a real system, we might notify waiting readers here

    def remove(self, lease_id: str):
        """Removes the object associated with the lease.
        Requires a CREATE lease (or a special DELETE intent if we had one).
        For now, we reuse CREATE intent to imply 'ownership' or 'write access'.
        """
        lease = self._get_active_lease(lease_id)
        if lease.access != AccessType.CREATE:
            raise ValueError("Cannot remove object with a read lease")
        
        object_id = lease.object_id
        if object_id in self.objects:
            # TODO: Handle other active leases on this object?
            # For now, we just delete it.
            del self.objects[object_id]
        
        # Release the lease itself
        self.release(lease_id)

    def release(self, lease_id: str):
        if lease_id not in self.leases:
            return
        lease = self.leases[lease_id]
        lease.release()
        del self.leases[lease_id]
        
        # Check if object should be evicted?
        # For now, we keep it.

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
        # Simple lazy cleanup
        expired = [lid for lid, l in self.leases.items() if l.is_expired()]
        for lid in expired:
            self.release(lid)

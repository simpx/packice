import os
import shutil
from typing import Optional, Dict, Any, Tuple
from collections import OrderedDict

from ..core.peer import Peer
from ..core.object import Object
from ..core.lease import Lease, AccessType
from ..backends.memory import MemBlob, MemoryLease
from ..backends.fs import FileBlob

class TieredPeer(Peer):
    """
    A Peer implementation that manages memory usage with LRU eviction.
    When memory is full, it evicts the least recently used object to the file system.
    """
    def __init__(self, data_dir: str, max_items: int = 100):
        # Initialize without factories, we will override create_* methods
        super().__init__()
        self.data_dir = data_dir
        self.max_items = max_items
        
        # LRU tracking: object_id -> timestamp (or just use OrderedDict for order)
        self.lru_order: OrderedDict[str, None] = OrderedDict()
        
        os.makedirs(self.data_dir, exist_ok=True)

    def create_blob(self, object_id: str):
        # By default, create in Memory
        return MemBlob(object_id)

    def create_lease(self, object_id: str, access: AccessType, ttl: Optional[float]):
        return MemoryLease(object_id, access, ttl)

    def acquire(self, object_id: Optional[str], access: AccessType, ttl: Optional[float] = None, meta: Optional[Dict[str, Any]] = None) -> Tuple[Lease, Object]:
        # 1. Update LRU on access
        if object_id and object_id in self.objects:
            self._mark_used(object_id)

        # 2. Check capacity before creating new object
        if access == AccessType.CREATE:
            self._ensure_capacity()

        # 3. Call parent implementation
        lease, obj = super().acquire(object_id, access, ttl, meta)
        
        # 4. If we just created it, track it
        if access == AccessType.CREATE:
            self._mark_used(obj.object_id)
            
        return lease, obj

    def _mark_used(self, object_id: str):
        """Move object to the end of the LRU list (most recently used)"""
        if object_id in self.lru_order:
            self.lru_order.move_to_end(object_id)
        else:
            self.lru_order[object_id] = None

    def _ensure_capacity(self):
        """Evict items if we are over capacity"""
        while len(self.objects) >= self.max_items:
            # Pop the first item (least recently used)
            victim_id, _ = self.lru_order.popitem(last=False)
            self._evict(victim_id)

    def _evict(self, object_id: str):
        """Evict object from Memory to Disk"""
        obj = self.objects.get(object_id)
        if not obj:
            return

        # Only evict if it's currently in memory (MemBlob)
        # In a real implementation, we'd check the blob type more robustly
        blob = obj.blobs[0]
        if not isinstance(blob, MemBlob):
            return # Already on disk or other type

        print(f"[TieredPeer] Evicting {object_id} to disk...")

        # 1. Create file path
        file_path = os.path.join(self.data_dir, object_id)
        
        # 2. Copy data (simplified)
        # MemBlob usually holds data in memory. We need to read it out.
        # Assuming blob.read() returns bytes
        data = blob.read() 
        with open(file_path, "wb") as f:
            f.write(data)
            
        # 3. Replace the blob in the object with a FileBlob
        new_blob = FileBlob(file_path)
        obj.blobs[0] = new_blob
        
        # 4. (Optional) Clean up the old memory blob
        # blob.close() or similar if needed

from typing import Optional, Dict, Any, Tuple, List
from ..core.peer import Peer
from ..core.object import Object
from ..core.lease import Lease, AccessType

class TieredPeer(Peer):
    """
    A composite Peer that manages a 'Hot' peer and a 'Cold' peer.
    Implements LRU eviction from Hot to Cold.
    """
    def __init__(self, hot_peer: Peer, cold_peer: Peer, max_items: int = 100):
        super().__init__()
        self.hot = hot_peer
        self.cold = cold_peer
        self.max_items = max_items
        self.lru_list: List[str] = []

    def acquire(self, object_id: Optional[str], access: AccessType, ttl: Optional[float] = None, meta: Optional[Dict[str, Any]] = None) -> Tuple[Lease, Object]:
        # 1. READ: Check Hot, then Cold
        if access == AccessType.READ:
            # Try hot first
            try:
                lease, obj = self.hot.acquire(object_id, access, ttl, meta)
                self._update_lru(object_id)
                return lease, obj
            except (KeyError, ValueError):
                pass
            
            # Try cold
            try:
                return self.cold.acquire(object_id, access, ttl, meta)
            except (KeyError, ValueError):
                pass
                
            raise KeyError(f"Object {object_id} not found in tiered storage")

        # 2. CREATE: Always create in Hot
        elif access == AccessType.CREATE:
            self._ensure_capacity()
            
            # We don't know the object_id yet if it's None, so we let hot peer generate it
            # But we need to know it to track LRU.
            # So we might need to peek or handle the return.
            lease, obj = self.hot.acquire(object_id, access, ttl, meta)
            self._update_lru(obj.object_id)
            return lease, obj

        # 3. WRITE: Check Hot, then Cold
        elif access == AccessType.WRITE:
            # Try hot first
            try:
                lease, obj = self.hot.acquire(object_id, access, ttl, meta)
                self._update_lru(object_id)
                return lease, obj
            except (KeyError, ValueError):
                pass
            
            # Try cold
            try:
                return self.cold.acquire(object_id, access, ttl, meta)
            except (KeyError, ValueError):
                pass
                
            raise KeyError(f"Object {object_id} not found in tiered storage")
            
        raise ValueError(f"Unknown access type: {access}")

    def seal(self, lease_id: str):
        try:
            self.hot.seal(lease_id)
            return
        except (KeyError, ValueError):
            pass
            
        try:
            self.cold.seal(lease_id)
            return
        except (KeyError, ValueError):
            pass
            
        raise KeyError(f"Lease {lease_id} not found")

    def discard(self, lease_id: str):
        try:
            if lease_id in self.hot.leases:
                lease = self.hot.leases[lease_id]
                if lease.object_id in self.lru_list:
                    self.lru_list.remove(lease.object_id)
                self.hot.discard(lease_id)
                return
        except Exception:
            pass

        try:
            self.cold.discard(lease_id)
            return
        except Exception:
            pass
            
        raise KeyError(f"Lease {lease_id} not found")

    def release(self, lease_id: str):
        self.hot.release(lease_id)
        self.cold.release(lease_id)

    def _update_lru(self, object_id: str):
        if object_id in self.lru_list:
            self.lru_list.remove(object_id)
        self.lru_list.append(object_id)

    def _ensure_capacity(self):
        while len(self.lru_list) >= self.max_items:
            victim_id = self.lru_list.pop(0)
            self._evict_to_cold(victim_id)

    def _evict_to_cold(self, object_id: str):
        print(f"[TieredPeer] Evicting {object_id} from Hot to Cold...")
        
        # 1. Read from Hot
        try:
            read_lease, hot_obj = self.hot.acquire(object_id, AccessType.READ)
        except KeyError:
            return

        blob_data = hot_obj.blobs[0].read(offset=0)
        self.hot.release(read_lease.lease_id)

        # 2. Write to Cold
        create_lease, cold_obj = self.cold.acquire(object_id, AccessType.CREATE)
        
        cold_obj.blobs[0].truncate(len(blob_data))
        
        # MemBlob.file is a file object, we can seek.
        if hasattr(cold_obj.blobs[0], 'file'):
             cold_obj.blobs[0].file.seek(0)
             cold_obj.blobs[0].write(blob_data)
        else:
             # Fallback if not MemBlob (though it should be)
             cold_obj.blobs[0].write(blob_data)
        
        self.cold.seal(create_lease.lease_id)
        self.cold.release(create_lease.lease_id)

        # 3. Remove from Hot
        lease, _ = self.hot.acquire(object_id, AccessType.WRITE)
        self.hot.discard(lease.lease_id)

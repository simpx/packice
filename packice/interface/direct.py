from typing import Any, Dict, Optional, List, Tuple
import os
from .base import Interface
from ..core.lease import AccessType

# Forward declaration for type hinting
try:
    from ..core.peer import Peer
except ImportError:
    Peer = Any

class DirectInterface(Interface):
    def __init__(self, peer: Peer):
        self.peer = peer

    def acquire(self, object_id: Optional[str], intent: str, ttl: Optional[float] = None, meta: Optional[Dict] = None) -> Tuple[Dict, List[Any]]:
        access = AccessType.CREATE if intent == 'create' else AccessType.READ
        lease, obj = self.peer.acquire(object_id, access, ttl, meta)
        
        handles = []
        for b in obj.blobs:
            h = b.get_handle()
            if isinstance(h, int):
                # Duplicate FD for the client so they own their copy
                handles.append(os.dup(h))
            else:
                handles.append(h)
                
        info = {
            "lease_id": lease.lease_id,
            "object_id": lease.object_id,
            "intent": intent,
            "ttl_seconds": ttl
        }
        return info, handles

    def seal(self, lease_id: str) -> None:
        self.peer.seal(lease_id)

    def release(self, lease_id: str) -> None:
        self.peer.release(lease_id)

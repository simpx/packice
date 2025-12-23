from typing import Any, Dict, Optional, List, Tuple
import os
from .base import Transport
from ..core.lease import AccessType

# Forward declaration for type hinting
try:
    from ..core.peer import Peer
except ImportError:
    Peer = Any

class DirectTransport(Transport):
    def __init__(self, peer: Peer):
        self.peer = peer

    def acquire(self, object_id: Optional[str], intent: str, ttl: Optional[float] = None, meta: Optional[Dict] = None) -> Tuple[Dict, List[Any]]:
        if intent == 'create':
            access = AccessType.CREATE
        elif intent == 'write':
            access = AccessType.WRITE
        else:
            access = AccessType.READ

        lease, obj = self.peer.acquire(object_id, access, ttl, meta)
        
        handles = []
        if obj:
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
            "ttl_seconds": ttl,
            "meta": obj.meta if obj else {}
        }
        return info, handles

    def seal(self, lease_id: str) -> None:
        self.peer.seal(lease_id)

    def discard(self, lease_id: str) -> None:
        self.peer.discard(lease_id)

    def release(self, lease_id: str) -> None:
        self.peer.release(lease_id)

from typing import Any, Dict, Optional, IO, List, Union, Tuple
import os
from .interface.base import Interface
from .interface.http import HttpInterface
from .interface.uds import UdsInterface
from .interface.direct import DirectInterface

# Forward declaration for type hinting
try:
    from .core.peer import Peer
except ImportError:
    Peer = Any

class Lease:
    def __init__(self, interface: Interface, info: Dict, handles: List[Any]):
        self.interface = interface
        self.info = info
        self.handles = handles
        self.lease_id = info['lease_id']
        self.object_id = info['object_id']

    def open(self, mode: str = "rb") -> IO:
        """
        Open the blob for reading or writing.
        Returns a file-like object.
        For now, assumes single blob or opens the last one (for writing).
        """
        if not self.handles:
            raise ValueError("No handles available")
        
        # For CREATE, we usually write to the last blob.
        # For READ, we might want to read the first?
        # This is a simplification.
        handle = self.handles[-1]

        if isinstance(handle, int):
            # It's an FD. Duplicate it so the file object doesn't close the original handle
            # which is managed by this Lease object.
            new_fd = os.dup(handle)
            f = os.fdopen(new_fd, mode)
            # Attempt to seek to 0 for read mode, as FD might share offset
            if 'r' in mode:
                try:
                    f.seek(0)
                except OSError:
                    pass
            return f
        elif isinstance(handle, str):
            # It's a path
            return open(handle, mode)
        else:
            raise ValueError(f"Unknown handle type: {type(handle)}")

    def seal(self):
        self.interface.seal(self.lease_id)

    def release(self):
        self.interface.release(self.lease_id)
        # If handles are FDs, close them
        for handle in self.handles:
            if isinstance(handle, int):
                try:
                    os.close(handle)
                except OSError:
                    pass

class Client:
    def __init__(self, target: Union[str, Peer]):
        """
        Initialize Client with an address or a Peer instance.
        If target is a Peer instance, uses DirectInterface.
        If target is a string:
            If starts with http:// or https://, uses HTTP interface.
            Otherwise, assumes it's a UDS socket path.
        """
        if isinstance(target, str):
            if target.startswith("http://") or target.startswith("https://"):
                self.interface = HttpInterface(target)
            else:
                self.interface = UdsInterface(target)
        else:
            # Assume it's a Peer instance
            self.interface = DirectInterface(target)

    def acquire(self, object_id: Optional[str] = None, intent: str = "read", ttl: int = 60, meta: dict = None) -> Lease:
        info, handles = self.interface.acquire(object_id, intent, ttl, meta)
        return Lease(self.interface, info, handles)

# Global registry for named in-process peers
_LOCAL_PEERS: Dict[str, Any] = {}

def _create_default_peer() -> Peer:
    from .core.peer import Peer
    from .backends.memory import MemBlob, MemoryLease
    
    return Peer(
        blob_factory=lambda oid: MemBlob(oid),
        lease_factory=lambda oid, acc, ttl: MemoryLease(oid, acc, ttl)
    )

def connect(target: Union[str, Peer, None] = None) -> Client:
    """
    Helper to create a Client.
    
    Usage:
    - connect(): Connects to a new, isolated in-memory Peer.
    - connect("memory://shared"): Connects to a named shared in-memory Peer.
    - connect("http://..."): Connects to a remote HTTP Peer.
    - connect("/tmp/..."): Connects to a local UDS Peer.
    - connect(peer_instance): Connects to an existing Peer instance.
    """
    if target is None:
        # Create a new isolated peer
        return Client(_create_default_peer())
    
    if isinstance(target, str) and target.startswith("memory://"):
        # Connect to a named shared peer
        name = target.replace("memory://", "")
        if not name:
            name = "default"
            
        if name not in _LOCAL_PEERS:
            _LOCAL_PEERS[name] = _create_default_peer()
        
        return Client(_LOCAL_PEERS[name])
    
    return Client(target)

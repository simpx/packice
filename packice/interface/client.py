from typing import Any, Dict, Optional, IO, List, Union, Tuple
import os
import mmap
from ..transport.base import Transport
from ..transport.http import HttpTransport
from ..transport.uds import UdsTransport
from ..transport.direct import DirectTransport

# Forward declaration for type hinting
try:
    from ..core.peer import Peer
except ImportError:
    Peer = Any

class Object:
    """
    Represents a PackIce object handle.
    Wraps the underlying lease and provides access to the object data.
    """
    def __init__(self, transport: Transport, info: Dict, handles: List[Any]):
        self.transport = transport
        self.info = info
        self.handles = handles
        self.lease_id = info['lease_id']
        self.object_id = info['object_id']
        self._buffer = None
        self._mmap = None
        self._file = None

    @property
    def id(self) -> str:
        return self.object_id

    @property
    def buffer(self) -> memoryview:
        if self._buffer is not None:
            return self._buffer
        
        if not self.handles:
            raise ValueError("No handles available")
        
        # Assuming single blob for now
        handle = self.handles[0]
        
        if isinstance(handle, int):
            # FD
            # We need to keep the FD open for mmap
            # We can use the FD directly
            # Check if it's read or write intent
            intent = self.info.get('intent', 'read')
            prot = mmap.PROT_READ
            flags = mmap.MAP_SHARED
            if intent in ('write', 'create'):
                prot |= mmap.PROT_WRITE
            
            # Get current size
            size = os.fstat(handle).st_size
            if size == 0:
                # Cannot mmap empty file usually, but maybe we want to allow it?
                # If it's create, user should have truncated it first?
                # Or we return empty memoryview?
                pass

            self._mmap = mmap.mmap(handle, 0, flags=flags, prot=prot)
            self._buffer = memoryview(self._mmap)
            
        elif isinstance(handle, str):
            # Path
            intent = self.info.get('intent', 'read')
            mode = "r+b" if intent in ('write', 'create') else "rb"
            self._file = open(handle, mode)
            
            prot = mmap.PROT_READ
            flags = mmap.MAP_SHARED
            if intent in ('write', 'create'):
                prot |= mmap.PROT_WRITE
                
            self._mmap = mmap.mmap(self._file.fileno(), 0, flags=flags, prot=prot)
            self._buffer = memoryview(self._mmap)
            
        return self._buffer

    def truncate(self, size: int):
        if not self.handles:
            raise ValueError("No handles available")
        
        handle = self.handles[0]
        if isinstance(handle, int):
            os.ftruncate(handle, size)
        elif isinstance(handle, str):
            with open(handle, "r+b") as f:
                f.truncate(size)
        
        # Invalidate buffer if size changed
        if self._buffer:
            self._buffer.release()
            self._buffer = None
        if self._mmap:
            self._mmap.close()
            self._mmap = None

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
        if self._buffer:
            self._buffer.release()
            self._buffer = None
        if self._mmap:
            self._mmap.close()
            self._mmap = None
        if self._file:
            self._file.close()
            self._file = None
            
        self.transport.seal(self.lease_id)

    def discard(self):
        if self._buffer:
            self._buffer.release()
            self._buffer = None
        if self._mmap:
            self._mmap.close()
            self._mmap = None
        if self._file:
            self._file.close()
            self._file = None

        self.transport.discard(self.lease_id)
        # If handles are FDs, close them
        for handle in self.handles:
            if isinstance(handle, int):
                try:
                    os.close(handle)
                except OSError:
                    pass

    def release(self):
        self.close()

    def close(self):
        if self._buffer:
            self._buffer.release()
            self._buffer = None
        if self._mmap:
            self._mmap.close()
            self._mmap = None
        if self._file:
            self._file.close()
            self._file = None

        self.transport.release(self.lease_id)
        # If handles are FDs, close them
        for handle in self.handles:
            if isinstance(handle, int):
                try:
                    os.close(handle)
                except OSError:
                    pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class Client:
    def __init__(self, target: Union[str, Peer]):
        """
        Initialize Client with an address or a Peer instance.
        If target is a Peer instance, uses DirectTransport.
        If target is a string:
            If starts with http:// or https://, uses HTTP transport.
            Otherwise, assumes it's a UDS socket path.
        """
        if isinstance(target, str):
            if target.startswith("http://") or target.startswith("https://"):
                self.transport = HttpTransport(target)
            else:
                self.transport = UdsTransport(target)
        else:
            # Assume it's a Peer instance
            self.transport = DirectTransport(target)

    def _acquire(self, object_id: Optional[str] = None, intent: str = "read", ttl: int = 60, meta: dict = None) -> Object:
        info, handles = self.transport.acquire(object_id, intent, ttl, meta)
        return Object(self.transport, info, handles)

    def create(self, size: int = 0, meta: dict = None) -> Object:
        """
        Create a new object.
        """
        obj = self._acquire(intent="create", meta=meta)
        if size > 0:
            obj.truncate(size)
        return obj

    def get(self, object_id: str) -> Object:
        """
        Get an existing object for reading.
        """
        return self._acquire(object_id, intent="read")

    def delete(self, object_id: str):
        """
        Helper to delete an object.
        Acquires a WRITE lease and then discards it.
        """
        obj = self._acquire(object_id, intent="write")
        obj.discard()

# Global registry for named in-process peers
_LOCAL_PEERS: Dict[str, Any] = {}

def _create_default_peer() -> Peer:
    from ..peers.memory import MemoryPeer
    
    return MemoryPeer()

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

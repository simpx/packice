from typing import Any, Dict, Optional, IO, List, Union, Tuple
import os
import mmap
from ..transport.base import Transport
from ..transport.http import HttpTransport
from ..transport.uds import UdsTransport
from ..transport.direct import DirectTransport
from ..backends.memory import MemoryBlobView
from ..backends.fs import FileBlobView

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
        self._blob = self._reconstruct_blob()

    def _reconstruct_blob(self):
        if not self.handles:
            raise ValueError("No handles available")
        
        # Assuming single blob for now
        handle = self.handles[0]
        intent = self.info.get('intent', 'read')
        mode = "r+b" if intent in ('write', 'create') else "rb"
        
        if isinstance(handle, int):
            # FD -> MemoryBlobView
            return MemoryBlobView(handle, mode=mode)
        elif isinstance(handle, str):
            # Path -> FileBlobView
            return FileBlobView(handle, mode=mode)
        elif isinstance(handle, dict) and handle.get('type') == 'shared_fs':
            from ..backends.shared_fs import SharedFSBlobView
            return SharedFSBlobView(handle['path'], mode=mode, data_offset=handle.get('data_offset', 0))
        else:
            raise ValueError(f"Unknown handle type: {type(handle)}")

    @property
    def id(self) -> str:
        return self.object_id

    @property
    def buffer(self) -> memoryview:
        return self._blob.memoryview()

    def truncate(self, size: int):
        self._blob.truncate(size)

    def write(self, data: bytes):
        """Write data to the object."""
        self._blob.write(data)

    def open(self, mode: str = "rb") -> IO:
        """
        Open the blob for reading or writing.
        Returns a file-like object.
        """
        # NativeBlob doesn't expose a file object directly in the interface,
        # but we can implement it or wrap the FD.
        # For now, let's just return a file object wrapping the FD if possible.
        handle = self._blob.get_handle()
        if isinstance(handle, int):
            # Duplicate FD to avoid closing the original when file object is closed?
            # Or just return a new file object.
            # If we dup, we need to manage it.
            new_fd = os.dup(handle)
            f = os.fdopen(new_fd, mode)
            try:
                f.seek(0)
            except OSError:
                # Seek might fail on some types of FDs (e.g. pipes), but for files it should work
                pass
            return f
        else:
            # Should not happen with NativeBlob
            raise NotImplementedError("Cannot open file-like object for this blob type")

    def get_meta(self, key: str) -> Any:
        """Get a metadata value."""
        if self.info and 'meta' in self.info and self.info['meta']:
            return self.info['meta'].get(key)
        return None

    def seal(self):
        self._blob.seal()
        self.transport.seal(self.lease_id)

    def discard(self):
        self._close()
        self.transport.discard(self.lease_id)

    def release(self):
        self._close()

    def _close(self):
        if self._blob:
            self._blob.close()
            self._blob = None
        self.transport.release(self.lease_id)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()

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

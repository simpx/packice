from ..core.peer import Peer
from ..backends.memory import MemBlob, MemoryLease

class MemoryPeer(Peer):
    """
    A Peer implementation that stores everything in memory.
    Uses MemBlob for data and MemoryLease for metadata.
    """
    def __init__(self):
        super().__init__(
            blob_factory=lambda oid: MemBlob(oid),
            lease_factory=lambda oid, acc, ttl: MemoryLease(oid, acc, ttl)
        )

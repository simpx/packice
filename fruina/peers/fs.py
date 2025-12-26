import os
from ..core.peer import Peer
from ..backends.fs import FileBlob
from ..backends.memory import MemoryLease

class FileSystemPeer(Peer):
    """
    A Peer implementation that stores data on the file system.
    Uses FileBlob for data and MemoryLease for metadata.
    """
    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        super().__init__(
            blob_factory=lambda oid: FileBlob(os.path.join(self.data_dir, oid)),
            lease_factory=lambda oid, acc, ttl: MemoryLease(oid, acc, ttl)
        )

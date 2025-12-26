from enum import Enum
from typing import Dict, Optional, Any, List
from .blob import Blob

class ObjectState(Enum):
    CREATING = "CREATING"
    SEALED = "SEALED"

class Object:
    def __init__(self, object_id: str, blobs: Optional[List[Blob]] = None, meta: Optional[Dict[str, Any]] = None):
        self.object_id = object_id
        self.blobs = blobs or []
        self.meta = meta or {}
        self.state = ObjectState.CREATING
        self.sealed_size: Optional[int] = None

    def add_blob(self, blob: Blob):
        self.blobs.append(blob)

    def seal(self):
        if self.state == ObjectState.SEALED:
            return
        for blob in self.blobs:
            blob.seal()
        self.state = ObjectState.SEALED

    def is_sealed(self) -> bool:
        return self.state == ObjectState.SEALED

    def delete(self):
        for blob in self.blobs:
            blob.delete()

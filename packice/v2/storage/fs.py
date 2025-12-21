import os
from typing import Any
from ..core.blob import Blob

class FileBlob(Blob):
    def __init__(self, path: str):
        self.path = path
        self.file = None
        self.is_sealed = False
        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Open for writing initially
        self.file = open(path, "wb+")

    def write(self, data: bytes) -> int:
        if self.is_sealed:
            raise ValueError("Blob is sealed")
        return self.file.write(data)

    def read(self, size: int = -1, offset: int = 0) -> bytes:
        self.file.seek(offset)
        return self.file.read(size)

    def seal(self) -> None:
        if self.is_sealed:
            return
        self.file.flush()
        self.file.close()
        # Re-open in read-only mode
        self.file = open(self.path, "rb")
        self.is_sealed = True

    def get_handle(self) -> Any:
        # For FS blob, the handle is the path
        return self.path

    def close(self) -> None:
        if self.file:
            self.file.close()

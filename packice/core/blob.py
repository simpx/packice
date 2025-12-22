from abc import ABC, abstractmethod
from typing import Any, Optional

class Blob(ABC):
    """
    Abstract representation of a data blob.
    """
    
    @abstractmethod
    def write(self, data: bytes) -> int:
        """Write data to the blob. Only allowed if mutable."""
        pass

    @abstractmethod
    def read(self, size: int = -1, offset: int = 0) -> bytes:
        """Read data from the blob."""
        pass

    @abstractmethod
    def truncate(self, size: int) -> None:
        """Resize the blob."""
        pass

    @abstractmethod
    def memoryview(self, mode: str = "rb") -> memoryview:
        """Return a memoryview of the blob."""
        pass

    @abstractmethod
    def seal(self) -> None:
        """Make the blob immutable."""
        pass

    @abstractmethod
    def get_handle(self) -> Any:
        """
        Return the underlying handle. 
        Could be a file path (str), a file descriptor (int), etc.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up resources."""
        pass

    @abstractmethod
    def delete(self) -> None:
        """Delete the blob data."""
        pass

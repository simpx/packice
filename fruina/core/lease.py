from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Any
from .object import Object

class AccessType(Enum):
    READ = "READ"
    CREATE = "CREATE"
    WRITE = "WRITE"

class Lease(ABC):
    """
    Abstract representation of a Lease.
    """
    
    @property
    @abstractmethod
    def lease_id(self) -> str:
        pass

    @property
    @abstractmethod
    def object_id(self) -> str:
        pass

    @property
    @abstractmethod
    def access(self) -> AccessType:
        pass

    @property
    @abstractmethod
    def ttl(self) -> Optional[float]:
        pass

    @abstractmethod
    def is_expired(self) -> bool:
        """Check if the lease is expired."""
        pass

    @abstractmethod
    def renew(self) -> None:
        """Renew the lease."""
        pass

    @abstractmethod
    def release(self) -> None:
        """Release the lease."""
        pass

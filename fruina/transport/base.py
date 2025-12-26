from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple, List

class Transport(ABC):
    @abstractmethod
    def acquire(self, object_id: Optional[str], intent: str, ttl: Optional[float] = None, meta: Optional[Dict] = None) -> Tuple[Dict, List[Any]]:
        """
        Returns (lease_info, blob_handles)
        lease_info should contain 'lease_id', 'object_id', etc.
        blob_handles is a list of paths (str) or fds (int).
        """
        pass

    @abstractmethod
    def seal(self, lease_id: str) -> None:
        pass

    @abstractmethod
    def discard(self, lease_id: str) -> None:
        pass

    @abstractmethod
    def release(self, lease_id: str) -> None:
        pass

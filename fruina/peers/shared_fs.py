import os
import time
import uuid
import threading
import logging
import json
import struct
import math
from typing import Optional, Dict, Any, Tuple, List
from pathlib import Path

from ..core.peer import Peer
from ..core.object import Object
from ..core.lease import Lease, AccessType
from ..backends.shared_fs import SharedFSBlob

logger = logging.getLogger(__name__)

class SharedFSLease(Lease):
    def __init__(self, lease_id: str, object_id: str, access: AccessType, ttl: int, file_path: Optional[Path] = None):
        self._lease_id = lease_id
        self._object_id = object_id
        self._access = access
        self._ttl = ttl
        self.file_path = file_path
        self.created_at = time.time()

    @property
    def lease_id(self) -> str:
        return self._lease_id

    @property
    def object_id(self) -> str:
        return self._object_id

    @property
    def access(self) -> AccessType:
        return self._access

    @property
    def ttl(self) -> Optional[float]:
        return self._ttl

    def is_expired(self) -> bool:
        # For SharedFS, expiration is handled by file mtime check in GC
        # But locally we can check time
        return (time.time() - self.created_at) > self._ttl

    def renew(self) -> None:
        # Update mtime of file to prevent GC
        if self.file_path and self.file_path.exists():
            os.utime(self.file_path, None)
        self.created_at = time.time()

    def release(self) -> None:
        pass

class SharedFSPeer(Peer):
    """
    A Peer implementation that uses a Shared Filesystem for data and metadata
    """
    def __init__(self, mount_point: str, capacity: int = 1000):
        self.root = Path(mount_point)
        self.data_dir = self.root / 'data'
        self.leases_dir = self.root / 'leases'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.leases_dir.mkdir(parents=True, exist_ok=True)
        
        self.capacity = capacity
        self._active_leases: Dict[str, SharedFSLease] = {}
        
        self._stop_maintenance = threading.Event()
        self._maintenance_thread = None

    def acquire(self, object_id: Optional[str], access: AccessType, ttl: Optional[float] = 300, meta: Optional[Dict[str, Any]] = None) -> Tuple[Lease, Object]:
        if object_id is None:
            object_id = str(uuid.uuid4())
        
        if access == AccessType.CREATE:
            # 1. Create lease file
            # Use object_id.lease_id as filename
            lease_id = str(uuid.uuid4())
            lease_filename = f"{object_id}.{lease_id}"
            lease_path = self.leases_dir / lease_filename
            
            # 2. Create Lease and Object
            meta = meta or {}
            # Convert TTL to milliseconds for storage
            # Initial TTL is the Lease TTL, to ensure cleanup if client crashes before sealing
            ttl_ms = int(ttl * 1000)
            blob = SharedFSBlob(str(lease_path), mode="wb+", meta=meta, ttl=ttl_ms)
            
            lease = SharedFSLease(lease_id, object_id, access, int(ttl), lease_path)
            self._active_leases[lease_id] = lease
            
            obj = Object(object_id, [blob], meta=meta)
            return lease, obj

        elif access == AccessType.WRITE:
            final_path = self.data_dir / object_id
            if not final_path.exists():
                raise FileNotFoundError(f"Object {object_id} not found")
            
            # Read Header to get Meta and DataOffset
            blob = SharedFSBlob(str(final_path), mode="r+b")
            meta = blob.get_meta()
            # data_offset is automatically read by SharedFSBlob.__init__ if not provided
            
            lease_id = str(uuid.uuid4())
            lease = SharedFSLease(lease_id, object_id, access, int(ttl), final_path)
            self._active_leases[lease_id] = lease
            
            obj = Object(object_id, [blob], meta=meta)
            return lease, obj

        elif access == AccessType.READ:
            final_path = self.data_dir / object_id
            if not final_path.exists():
                raise FileNotFoundError(f"Object {object_id} not found")
            
            blob = SharedFSBlob(str(final_path), mode="rb")
            meta = blob.get_meta()

            lease_id = str(uuid.uuid4())
            lease = SharedFSLease(lease_id, object_id, access, int(ttl))
            
            obj = Object(object_id, [blob], meta=meta)
            return lease, obj
            
        raise ValueError(f"Unsupported access type: {access}")

    def seal(self, lease_id: str):
        lease = self._active_leases.get(lease_id)
        if not lease:
            raise ValueError("Invalid or expired lease")
        
        if lease.access != AccessType.CREATE and lease.access != AccessType.WRITE:
            raise ValueError("Cannot seal a read lease")
            
        if not lease.file_path or not lease.file_path.exists():
            raise ValueError("Lease file missing")
            
        if lease.access == AccessType.CREATE:
            final_path = self.data_dir / lease.object_id
            
            try:
                with SharedFSBlob(str(lease.file_path), mode="r+b") as blob:
                    # Check for Object TTL in metadata
                    meta = blob.get_meta()
                    object_ttl = meta.get('ttl')
                    
                    new_ttl_ms = 0 # Default to 0 (no expiration) for sealed objects
                    if object_ttl is not None:
                        new_ttl_ms = int(float(object_ttl) * 1000)
                    
                    blob.seal(new_ttl=new_ttl_ms)
            except Exception as e:
                logger.error(f"Failed to update header for seal: {e}")
                raise

            os.rename(lease.file_path, final_path)
            
            lease.file_path = None
        
        elif lease.access == AccessType.WRITE:
            pass

        del self._active_leases[lease_id]

    def discard(self, lease_id: str):
        lease = self._active_leases.get(lease_id)
        if not lease:
            return

        if lease.file_path and lease.file_path.exists():
            try:
                os.remove(lease.file_path)
            except OSError:
                pass
        
        del self._active_leases[lease_id]

    def release(self, lease_id: str):
        lease = self._active_leases.get(lease_id)
        if not lease:
            return

        if lease.access == AccessType.CREATE:
            if lease.file_path and lease.file_path.exists():
                try:
                    os.remove(lease.file_path)
                except OSError:
                    pass
        
        del self._active_leases[lease_id]

    # --- Maintenance Logic ---

    def start_maintenance(self, interval: int = 60):
        if self._maintenance_thread and self._maintenance_thread.is_alive():
            return

        self._stop_maintenance.clear()
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            args=(interval,),
            daemon=True,
            name="SharedFSPeer-Maintenance"
        )
        self._maintenance_thread.start()
        logger.info("SharedFSPeer maintenance thread started")

    def stop_maintenance(self):
        if self._maintenance_thread:
            self._stop_maintenance.set()
            self._maintenance_thread.join()
            logger.info("SharedFSPeer maintenance thread stopped")

    def _maintenance_loop(self, interval: int):
        while not self._stop_maintenance.is_set():
            try:
                self._cleanup_zombies()
            except Exception as e:
                logger.error(f"Error in maintenance loop: {e}")
            
            time.sleep(interval)

    def _cleanup_zombies(self):
        """
        Cleanup old lease files and expired objects.
        """
        now = time.time()
        
        dirs_to_clean = []
        if self.leases_dir.exists():
            dirs_to_clean.append(self.leases_dir)
        if self.data_dir.exists():
            dirs_to_clean.append(self.data_dir)

        for d in dirs_to_clean:
            for item in d.iterdir():
                if item.is_file():
                    try:
                        try:
                            # Use context manager since we added it
                            with SharedFSBlob(str(item), mode="rb") as blob:
                                ttl_ms = blob.get_ttl()
                        except Exception:
                            # Default TTL for unknown files (e.g. 1 hour)
                            ttl_ms = 3600 * 1000
                        
                        ttl_sec = ttl_ms / 1000.0
                        stat = item.stat()
                        
                        # If TTL is 0, it means no expiration (unless it's a zombie lease, but leases always have TTL)
                        # Sealed objects with 0 TTL live forever.
                        if ttl_sec > 0 and (now - stat.st_mtime > ttl_sec):
                            logger.info(f"Removing expired file: {item} (TTL: {ttl_sec}s)")
                            os.remove(item)
                    except OSError:
                        pass

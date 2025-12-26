import os
import mmap
import struct
import json
from typing import Any, Dict, Tuple, Optional
from ..core.blob import Blob, BlobView

# Header Format: Magic(8s), Version(H), Flags(B), TTL(I), Reserved(1x), MetaLen(Q), DataOffset(Q)
# Total Size: 8 + 2 + 1 + 4 + 1 + 8 + 8 = 32 bytes
HEADER_STRUCT = struct.Struct("!8sHBIxQQ")
HEADER_SIZE = HEADER_STRUCT.size
MAGIC = b'FRUINA!!'
ALIGNMENT = 4096
FLAG_SEALED = 0x01

class SharedFSBlobView(BlobView):
    """
    Client-side view of a SharedFSBlob.
    Wraps a file path and offset to access the data portion of the file.
    """
    def __init__(self, path: str, mode: str = "rb", data_offset: int = 0):
        self.path = path
        self.mode = mode
        self.data_offset = data_offset
        self.file = None
        self.is_sealed = False
        
        self.file = open(path, mode)
        
        if self.data_offset == 0:
            self._read_header_offset()
            
        if self.data_offset > 0:
            self.file.seek(self.data_offset)

    def _read_header_offset(self):
        current_pos = self.file.tell()
        try:
            self.file.seek(0)
            header_bytes = self.file.read(HEADER_SIZE)
            
            if len(header_bytes) < HEADER_SIZE:
                return
                
            magic, ver, flags, ttl, meta_len, data_offset = HEADER_STRUCT.unpack(header_bytes)
            if magic == MAGIC:
                self.data_offset = data_offset
                if flags & FLAG_SEALED:
                    self.is_sealed = True
            else:
                self.data_offset = 0
        except struct.error:
            self.data_offset = 0
        finally:
            self.file.seek(current_pos)

    def write(self, data: bytes) -> int:
        if self.is_sealed:
            raise ValueError("Blob is sealed")
        if 'w' not in self.mode and '+' not in self.mode:
            raise IOError("Blob not opened for writing")
        return self.file.write(data)

    def read(self, size: int = -1, offset: int = 0) -> bytes:
        self.file.seek(self.data_offset + offset)
        return self.file.read(size)

    def truncate(self, size: int) -> None:
        if self.is_sealed:
            raise ValueError("Blob is sealed")
        self.file.truncate(self.data_offset + size)
        self.file.flush()

    def memoryview(self, mode: str = "rb") -> memoryview:
        prot = mmap.PROT_READ
        if 'w' in mode or '+' in mode:
            prot |= mmap.PROT_WRITE
        
        try:
            length = 0
            offset = self.data_offset
            
            if offset % mmap.ALLOCATIONGRANULARITY != 0:
                mm = mmap.mmap(self.file.fileno(), 0, prot=prot)
                return memoryview(mm)[offset:]
            
            mm = mmap.mmap(self.file.fileno(), length, offset=offset, prot=prot)
            return memoryview(mm)
        except ValueError:
            if os.fstat(self.file.fileno()).st_size == 0:
                return memoryview(b"")
            raise

    def seal(self) -> None:
        """
        Sets the sealed flag in the header.
        """
        if self.is_sealed:
            return

        if 'w' not in self.mode and '+' not in self.mode:
             raise IOError("Blob not opened for writing")

        current_pos = self.file.tell()
        try:
            self.file.seek(0)
            header_bytes = self.file.read(HEADER_SIZE)
            if len(header_bytes) == HEADER_SIZE:
                magic, ver, flags, ttl, meta_len, data_offset = HEADER_STRUCT.unpack(header_bytes)
                if magic == MAGIC:
                    flags |= FLAG_SEALED
                    new_header = HEADER_STRUCT.pack(magic, ver, flags, ttl, meta_len, data_offset)
                    self.file.seek(0)
                    self.file.write(new_header)
                    self.file.flush()
                    self.is_sealed = True
        except Exception:
            pass
        finally:
            self.file.seek(current_pos)

    def close(self) -> None:
        if self.is_sealed:
            pass
        try:
            self.file.flush()
        except ValueError:
            pass
        self.file.close()

    def get_handle(self) -> Dict[str, Any]:
        return {
            'type': 'shared_fs',
            'path': self.path,
            'data_offset': self.data_offset,
        }

    def delete(self) -> None:
        self.close()
        if os.path.exists(self.path):
            try:
                os.remove(self.path)
            except OSError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SharedFSBlob(Blob):
    """
    A Blob implementation for Shared Filesystem.
    Used by the Peer to create files with headers.
    """
    def __init__(self, path: str, mode: str = "rb", data_offset: int = 0, meta: Optional[Dict[str, Any]] = None, ttl: int = 0):
        self.path = path
        self.mode = mode
        self.data_offset = data_offset
        self.ttl = ttl
        self.file = None
        self.is_sealed = False
        
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        
        self.file = open(path, mode)
        
        if meta is not None:
            self._write_header(meta)
        elif 'r' in mode and 'w' not in mode and data_offset == 0:
             self._read_header_offset()
            
        if self.data_offset > 0:
            self.file.seek(self.data_offset)

    def _write_header(self, meta: Dict[str, Any]):
        meta_json = json.dumps(meta).encode('utf-8')
        meta_len = len(meta_json)

        raw_header_size = HEADER_SIZE + meta_len

        self.data_offset = (raw_header_size + ALIGNMENT - 1) & ~(ALIGNMENT - 1)
        padding_len = self.data_offset - raw_header_size
        
        header_bytes = HEADER_STRUCT.pack(MAGIC, 1, 0, self.ttl, meta_len, self.data_offset)
        self.file.write(header_bytes)
        self.file.write(meta_json)
        if padding_len > 0:
            self.file.write(b'\0' * padding_len)

    def _read_header_offset(self):
        current_pos = self.file.tell()
        try:
            self.file.seek(0)
            header_bytes = self.file.read(HEADER_SIZE)
            
            if len(header_bytes) < HEADER_SIZE:
                return
                
            magic, ver, flags, ttl, meta_len, data_offset = HEADER_STRUCT.unpack(header_bytes)
            if magic == MAGIC:
                self.data_offset = data_offset
                if flags & FLAG_SEALED:
                    self.is_sealed = True
            else:
                self.data_offset = 0
        except struct.error:
            self.data_offset = 0
        finally:
            self.file.seek(current_pos)

    def get_ttl(self) -> int:
        current_pos = self.file.tell()
        try:
            self.file.seek(0)
            header_bytes = self.file.read(HEADER_SIZE)
            if len(header_bytes) < HEADER_SIZE:
                return 0
            magic, ver, flags, ttl, meta_len, data_offset = HEADER_STRUCT.unpack(header_bytes)
            if magic == MAGIC:
                return ttl
            return 0
        finally:
            self.file.seek(current_pos)

    def get_meta(self) -> Dict[str, Any]:
        current_pos = self.file.tell()
        try:
            self.file.seek(0)
            header_bytes = self.file.read(HEADER_SIZE)
            if len(header_bytes) < HEADER_SIZE:
                return {}
            
            magic, ver, flags, ttl, meta_len, data_offset = HEADER_STRUCT.unpack(header_bytes)
            if magic != MAGIC:
                return {}
            
            if meta_len > 0:
                meta_json = self.file.read(meta_len)
                return json.loads(meta_json)
            return {}
        except Exception:
            return {}
        finally:
            self.file.seek(current_pos)

    def seal(self, new_ttl: Optional[int] = None) -> None:
        """
        Sets the sealed flag in the header.
        Optionally updates the TTL.
        """
        if 'w' not in self.mode and '+' not in self.mode:
             raise IOError("Blob not opened for writing")

        current_pos = self.file.tell()
        try:
            self.file.seek(0)
            header_bytes = self.file.read(HEADER_SIZE)
            if len(header_bytes) == HEADER_SIZE:
                magic, ver, flags, ttl, meta_len, data_offset = HEADER_STRUCT.unpack(header_bytes)
                if magic == MAGIC:
                    flags |= FLAG_SEALED
                    
                    if new_ttl is not None:
                        ttl = new_ttl
                        
                    new_header = HEADER_STRUCT.pack(magic, ver, flags, ttl, meta_len, data_offset)
                    self.file.seek(0)
                    self.file.write(new_header)
                    self.file.flush()
                    self.is_sealed = True
        except Exception:
            pass
        finally:
            self.file.seek(current_pos)

    def write(self, data: bytes) -> int:
        if self.is_sealed:
            raise ValueError("Blob is sealed")
        if 'w' not in self.mode and '+' not in self.mode:
            raise IOError("Blob not opened for writing")
        return self.file.write(data)

    def read(self, size: int = -1, offset: int = 0) -> bytes:
        self.file.seek(self.data_offset + offset)
        return self.file.read(size)

    def truncate(self, size: int) -> None:
        if self.is_sealed:
            raise ValueError("Blob is sealed")
        self.file.truncate(self.data_offset + size)
        self.file.flush()

    def memoryview(self, mode: str = "rb") -> memoryview:
        prot = mmap.PROT_READ
        if 'w' in mode or '+' in mode:
            prot |= mmap.PROT_WRITE
        
        try:
            length = 0 
            offset = self.data_offset
            
            if offset % mmap.ALLOCATIONGRANULARITY != 0:
                mm = mmap.mmap(self.file.fileno(), 0, prot=prot)
                return memoryview(mm)[offset:]
            
            mm = mmap.mmap(self.file.fileno(), length, offset=offset, prot=prot)
            return memoryview(mm)
        except ValueError:
            if os.fstat(self.file.fileno()).st_size == 0:
                return memoryview(b"")
            raise

    def close(self) -> None:
        if self.is_sealed:
            pass
        try:
            self.file.flush()
        except ValueError:
            pass
        self.file.close()

    def get_handle(self) -> Dict[str, Any]:
        return {
            'type': 'shared_fs',
            'path': self.path,
            'data_offset': self.data_offset,
        }

    def delete(self) -> None:
        self.close()
        if os.path.exists(self.path):
            try:
                os.remove(self.path)
            except OSError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

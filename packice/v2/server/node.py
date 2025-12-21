import os
import time
from typing import Optional
from ..core.peer import Peer
from ..storage.fs import FileBlob
from ..storage.memory import MemBlob
from ..storage.memory_lease import MemoryLease
from ..transport.http_server import HttpServer
from ..transport.uds_server import UdsServer

class Node:
    def __init__(self, impl: str = "fs", transport: str = "http", 
                 port: int = 8080, socket_path: str = "/tmp/packice.sock", 
                 data_dir: str = "./data"):
        self.impl = impl
        self.transport = transport
        self.port = port
        self.socket_path = socket_path
        self.data_dir = data_dir
        self.peer: Optional[Peer] = None
        self.server = None

    def start(self):
        # 1. Setup Blob Factory
        if self.impl == "fs":
            data_dir = os.path.abspath(self.data_dir)
            os.makedirs(data_dir, exist_ok=True)
            print(f"Using FileBlob implementation in {data_dir}")
            
            def blob_factory(object_id: str):
                path = os.path.join(data_dir, object_id)
                return FileBlob(path)
                
        elif self.impl == "mem":
            print("Using MemBlob implementation")
            
            def blob_factory(object_id: str):
                return MemBlob(object_id)
        else:
            raise ValueError(f"Unknown impl: {self.impl}")
        
        # 2. Setup Lease Factory
        def lease_factory(object_id, access, ttl):
            return MemoryLease(object_id, access, ttl)

        # 3. Initialize Peer
        self.peer = Peer(blob_factory, lease_factory)
        
        # 4. Start Server
        if self.transport == "http":
            self.server = HttpServer(self.peer, port=self.port)
            self.server.start()
        elif self.transport == "uds":
            self.server = UdsServer(self.peer, socket_path=self.socket_path)
            self.server.start()
        else:
            raise ValueError(f"Unknown transport: {self.transport}")
            
        print("Node started. Press Ctrl+C to stop.")

    def wait(self):
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        print("Stopping...")
        if self.server:
            self.server.stop()

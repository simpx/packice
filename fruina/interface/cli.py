import argparse
import time
import sys
import os
from ..peers.memory import MemoryPeer
from ..peers.fs import FileSystemPeer
from ..transport.http import HttpServer
from ..transport.uds import UdsServer

def main():
    parser = argparse.ArgumentParser(description="Fruina Peer")
    parser.add_argument("--impl", choices=["fs", "mem"], default="fs", help="Blob implementation")
    parser.add_argument("--transport", choices=["http", "uds"], default="http", help="Transport protocol")
    parser.add_argument("--port", type=int, default=8080, help="HTTP port")
    parser.add_argument("--socket", default="/tmp/fruina.sock", help="UDS socket path")
    parser.add_argument("--data-dir", default="./data", help="Data directory for FS impl")
    
    args = parser.parse_args()

    # 1. Build Peer
    if args.impl == "fs":
        print(f"Using FileSystemPeer in {args.data_dir}")
        peer = FileSystemPeer(args.data_dir)
    elif args.impl == "mem":
        print("Using MemoryPeer")
        peer = MemoryPeer()
    else:
        raise ValueError(f"Unknown impl: {args.impl}")
    
    # 2. Start Server
    if args.transport == "http":
        server = HttpServer(peer, port=args.port)
        server.start()
    elif args.transport == "uds":
        server = UdsServer(peer, socket_path=args.socket)
        server.start()
    else:
        raise ValueError(f"Unknown transport: {args.transport}")
        
    print("Node started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
        server.stop()

if __name__ == "__main__":
    main()

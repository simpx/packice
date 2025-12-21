import argparse
import time
import sys
from ..server.node import Node

def main():
    parser = argparse.ArgumentParser(description="PackIce v2 Node")
    parser.add_argument("--impl", choices=["fs", "mem"], default="fs", help="Blob implementation")
    parser.add_argument("--transport", choices=["http", "uds"], default="http", help="Transport protocol")
    parser.add_argument("--port", type=int, default=8080, help="HTTP port")
    parser.add_argument("--socket", default="/tmp/packice.sock", help="UDS socket path")
    parser.add_argument("--data-dir", default="./data", help="Data directory for FS impl")
    
    args = parser.parse_args()

    # Initialize Node
    node = Node(
        impl=args.impl,
        transport=args.transport,
        port=args.port,
        socket_path=args.socket,
        data_dir=args.data_dir
    )
    
    # Start Node
    node.start()
    
    print("Node started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
        node.stop()

if __name__ == "__main__":
    main()

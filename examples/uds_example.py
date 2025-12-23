import sys
import os
import time
import threading
import signal
import multiprocessing

sys.path.append(os.getcwd())

import packice
from packice.peers.memory import MemoryPeer
from packice.transport.uds import UdsServer

SOCKET_PATH = "/tmp/packice_example_combined.sock"

def run_server():
    """Function to run the server in a separate process."""
    try:
        # Create the peer
        peer = MemoryPeer()
        print(f"[Server] Initialized MemoryPeer: {peer}")
        
        # Create and start the UDS server
        server = UdsServer(peer, socket_path=SOCKET_PATH)
        server.start()
        
        print(f"[Server] Running on {SOCKET_PATH}")
        
        # Keep server alive until interrupted
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Server] Stopping...")
    finally:
        if 'server' in locals():
            server.stop()

def run_client():
    """Function to run the client logic."""
    # Wait a bit for server to start
    time.sleep(1)
    
    print(f"\n[Client] Connecting to {SOCKET_PATH}...")

    try:
        client = packice.connect(SOCKET_PATH)
    except Exception as e:
        print(f"[Client] Failed to connect: {e}")
        return

    print(f"[Client] Connected!")

    # 2. Write API: create -> write -> seal
    print("\n[Client] --- Creating Object ---")
    data = b"Hello from separate process!"
    
    try:
        writer = client.create(size=len(data), meta={"content_type": "text/plain"})
        print(f"[Client] Created object: {writer.id}")
        
        # Direct memory access via property (shared memory via mmap over FD)
        writer.buffer[:] = data
        print(f"[Client] Wrote {len(data)} bytes directly to shared memory")
        
        # Explicitly seal to make immutable
        writer.seal()
        print("[Client] Sealed object")

        # 3. Read API: get -> read
        print("\n[Client] --- Reading Object ---")
        reader = client.get(writer.id)
        print(f"[Client] Got object: {reader.id}")
        
        # Zero-copy read
        content = bytes(reader.buffer)
        print(f"[Client] Read content from shared memory: {content}")
        assert content == data
        
        # Explicitly release resource
        reader.close()
        print("[Client] Released lease")

        # 4. Delete API
        print("\n[Client] --- Deleting Object ---")
        client.delete(writer.id)
        print(f"[Client] Deleted object")

        # Verify deletion
        try:
            client.get(writer.id)
        except Exception as e:
            print(f"[Client] Verified: Object not found (Error: {e})")

    except Exception as e:
        print(f"[Client] Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("=== Example 3: UDS Client/Server (Multiprocessing) ===")
    
    # Start server process
    server_process = multiprocessing.Process(target=run_server)
    server_process.start()
    
    try:
        # Run client logic in main process
        run_client()
    finally:
        # Cleanup
        print("\n[Main] Terminating server process...")
        server_process.terminate()
        server_process.join()
        
        # Cleanup socket file if it still exists
        if os.path.exists(SOCKET_PATH):
            try:
                os.unlink(SOCKET_PATH)
            except OSError:
                pass

if __name__ == "__main__":
    main()

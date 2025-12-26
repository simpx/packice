import sys
import os

sys.path.append(os.getcwd())

import fruina

def main():
    print("=== Example 1: Local In-Process Peer (Dream API) ===")

    # 1. Connect to in-process MemoryPeer
    peer = fruina.MemoryPeer()
    client = fruina.connect(peer)
    print(f"[*] Client connected to {peer}")

    # 2. Write API: create -> write -> seal
    print("\n--- Creating Object ---")
    data = b"Hello, Fruina!"
    
    writer = client.create(size=len(data), meta={"content_type": "text/plain"})
    print(f"[*] Created object: {writer.id}")
    
    # Direct memory access via property
    writer.buffer[:] = data
    print(f"[*] Wrote {len(data)} bytes directly to memory")
    
    # Explicitly seal to make immutable
    writer.seal()
    print("[*] Sealed object")

    # 3. Read API: get -> read
    print("\n--- Reading Object ---")
    reader = client.get(writer.id)
    print(f"[*] Got object: {reader.id}")
    
    # Zero-copy read
    content = bytes(reader.buffer)
    print(f"[*] Read content from memory: {content}")
    assert content == data
    
    # Explicitly release resource
    reader.close()
    print("[*] Released lease")

    # 4. Delete API
    print("\n--- Deleting Object ---")
    client.delete(writer.id)
    print(f"[*] Deleted object")

    # Verify deletion
    try:
        client.get(writer.id)
    except KeyError:
        print("[*] Verified: Object not found")

if __name__ == "__main__":
    main()

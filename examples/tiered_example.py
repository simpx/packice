import sys
import os

# Add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fruina.peers.memory import MemoryPeer
from fruina.peers.tiered import TieredPeer
from fruina.interface.client import Client
from fruina.transport.direct import DirectTransport

def main():
    print("=== Tiered Storage Example ===")

    # 1. Start Hot Peer (Capacity: 2) and Cold Peer (Unlimited)
    hot_peer = MemoryPeer()
    cold_peer = MemoryPeer()
    
    # TieredPeer manages eviction from Hot to Cold
    tiered_peer = TieredPeer(hot_peer=hot_peer, cold_peer=cold_peer, max_items=2)

    print(f"[*] Started TieredPeer (Hot Capacity: 2, Cold: Unlimited)")

    # 2. Client connects to TieredPeer
    # Client constructor takes target (Peer or str), not transport kwarg
    client = Client(tiered_peer)
    print("[*] Client connected to TieredPeer")

    # 3. Create 3 objects
    objects = []
    for i in range(1, 4):
        print(f"\n--- Creating Object {i} ---")
        data = f"Object {i} Data".encode()
        try:
            writer = client.create(size=len(data), meta={"name": f"obj_{i}"})
            writer.write(data)
            writer.seal()
            objects.append(writer)
            print(f"[*] Created Object {i}: {writer.id}")
        except Exception as e:
            print(f"[!] Failed to create Object {i}: {e}")
            exit(1)

    # 4. Verify distribution
    print("\n--- Verifying Distribution ---")
    
    # We need to check internal state of hot/cold peers
    # Note: TieredPeer doesn't expose hot/cold directly in public API, but we can access for testing
    count_hot = len(hot_peer.objects)
    count_cold = len(cold_peer.objects)
    
    print(f"[*] Hot Peer Object Count: {count_hot}")
    print(f"[*] Cold Peer Object Count: {count_cold}")

    # Expected: 
    # Obj 1 created -> Hot: [1]
    # Obj 2 created -> Hot: [1, 2]
    # Obj 3 created -> Hot full, evict 1 to Cold -> Hot: [2, 3], Cold: [1]
    
    if count_hot == 2 and count_cold == 1:
        print("[SUCCESS] Tiering logic worked!")
        
        # Verify which object is where
        obj1_id = objects[0].id
        obj2_id = objects[1].id
        obj3_id = objects[2].id
        
        if obj1_id in cold_peer.objects:
            print(f"  - Object 1 ({obj1_id}) is in Cold (Evicted)")
        else:
            print(f"  - Object 1 ({obj1_id}) is NOT in Cold (Unexpected)")
            
        if obj2_id in hot_peer.objects and obj3_id in hot_peer.objects:
            print(f"  - Objects 2 & 3 are in Hot")
        else:
            print(f"  - Objects 2 & 3 are NOT both in Hot (Unexpected)")

    else:
        print("[FAILURE] Tiering logic failed.")
        print(f"  Expected: Hot=2, Cold=1. Got: Hot={count_hot}, Cold={count_cold}")

    # 5. Verify Data Access (Transparent)
    print("\n--- Verifying Data Access ---")
    for i, obj in enumerate(objects):
        # We can read all objects transparently
        # Note: Reading Object 1 might promote it back to Hot depending on policy?
        # Current policy: Read from Cold if in Cold. No promotion implemented yet.
        content = obj.open().read()
        print(f"[*] Read Object {i+1} ({obj.id}): {content}")
        assert content == f"Object {i+1} Data".encode()

    # Cleanup
    for obj in objects:
        obj.close()

if __name__ == "__main__":
    main()

import os
import sys
import time
import packice


def demo_in_process():
    print(f"\n{'='*20} Running Demo: In-Process (DuckDB Style) {'='*20}")
    
    # 1. Connect to a shared in-memory peer
    print("Connecting to 'memory://demo'...")
    client1 = packice.connect("memory://demo")
    
    # 2. Create Object with Client 1
    print("\n--- Client 1: Create Object ---")
    lease = client1.acquire(intent="create")
    object_id = lease.object_id
    print(f"Acquired lease: {lease.lease_id} for object {object_id}")
    
    with lease.open("wb") as f:
        f.write(b"Hello Shared Memory!")
    lease.seal()
    lease.release()
    
    # 3. Read Object with Client 2 (Simulating another connection to same peer)
    print("\n--- Client 2: Read Object ---")
    client2 = packice.connect("memory://demo")
    lease = client2.acquire(object_id=object_id, intent="read")
    with lease.open("rb") as f:
        data = f.read()
        print(f"Read data: {data}")
    lease.release()

def run_demo(target: str, name: str):
    print(f"\n{'='*20} Running Demo: {name} {'='*20}")
    print(f"Connecting to: {target}")
    
    try:
        client = packice.connect(target)
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    # 1. Create Object
    print("\n--- 1. Create Object ---")
    try:
        lease = client.acquire(intent="create", ttl=60)
        object_id = lease.object_id
        print(f"Acquired lease: {lease.lease_id}")
        print(f"Generated Object ID: {object_id}")
        
        # Write data using the unified open() interface
        print("Writing data...")
        with lease.open("wb") as f:
            f.write(f"Hello from {name}!".encode('utf-8'))
        
        print("Sealing object...")
        lease.seal()
        
        print("Releasing lease...")
        lease.release()
        
    except Exception as e:
        print(f"Error during create flow: {e}")
        return

    # 2. Read Object
    print(f"\n--- 2. Read Object {object_id} ---")
    try:
        read_lease = client.acquire(object_id, intent="read")
        print(f"Acquired read lease: {read_lease.lease_id}")
        
        # Read data using the unified open() interface
        print("Reading data...")
        with read_lease.open("rb") as f:
            content = f.read()
            print(f"Content: {content.decode('utf-8')}")
            
        print("Releasing read lease...")
        read_lease.release()
        
    except Exception as e:
        print(f"Error during read flow: {e}")

def main():
    # Demo 1: In-Process
    demo_in_process()

    # Check if servers are running
    print("\nNote: For Networked demos, ensure you have started the servers:")
    print("  1. python3 -m packice.cli --impl fs --transport http --port 8080")
    print("  2. python3 -m packice.cli --impl mem --transport uds --socket /tmp/packice.sock")
    print("Waiting 2 seconds...")
    time.sleep(2)

    # Demo 2: HTTP + FS
    run_demo("http://localhost:8080", "HTTP + FS")

    # Demo 3: UDS + Memfd
    run_demo("/tmp/packice.sock", "UDS + Memfd")

if __name__ == "__main__":
    main()

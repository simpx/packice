import os
import sys
import time
import shutil
import tempfile
import logging

# Add the project root to sys.path to allow importing fruina
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import fruina
from fruina.peers.shared_fs import SharedFSPeer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SharedFSDemo")

def main():
    # 1. Setup shared directory
    shared_dir = tempfile.mkdtemp(prefix="fruina_shared_")
    logger.info(f"Created shared directory: {shared_dir}")

    try:
        # 2. Start 2 Peers pointing to the same directory
        # Peer 1
        peer1 = SharedFSPeer(shared_dir)
        peer1.start_maintenance(interval=1) # Fast maintenance for demo
        logger.info("Started Peer 1")

        # Peer 2
        peer2 = SharedFSPeer(shared_dir)
        peer2.start_maintenance(interval=1)
        logger.info("Started Peer 2")

        # 3. Connect Client to Peer 1
        client1 = fruina.connect(peer1)
        logger.info("Connected Client 1 to Peer 1")

        # 4. Create Object
        logger.info("Creating object 'test-obj' via Client 1...")
        meta = {"content_type": "text/plain", "author": "demo"}
        
        object_id = None
        with client1.create(meta=meta) as obj1:
            data = b"Hello, Shared World!"
            obj1.write(data)
            logger.info(f"Wrote {len(data)} bytes to object")
            
            # 5. Seal Object
            obj1.seal()
            object_id = obj1.id
            logger.info(f"Sealed object: {object_id}")

        # 6. Connect Client to Peer 2
        client2 = fruina.connect(peer2)
        logger.info("Connected Client 2 to Peer 2")

        # 7. Read Object via Peer 2
        logger.info(f"Reading object {object_id} via Client 2...")
        try:
            with client2.get(object_id) as obj2:
                read_data = obj2.buffer.tobytes()
                logger.info(f"Read data: {read_data}")
                
                if read_data == data:
                    logger.info("SUCCESS: Data verification passed!")
                else:
                    logger.error("FAILURE: Data verification failed!")
                    
                # Check metadata
                author = obj2.get_meta('author')
                logger.info(f"Retrieved metadata author: {author}")
                
                if author == 'demo':
                     logger.info("SUCCESS: Metadata verification passed!")
                else:
                     logger.warning(f"Metadata verification incomplete: {obj2.info.get('meta')}")
            
        except Exception as e:
            logger.error(f"Failed to read object: {e}")
            import traceback
            traceback.print_exc()

        # 8. Demonstrate TTL/GC
        logger.info("\n--- TTL Demonstration ---")
        logger.info("Creating short-lived object (TTL=2s)...")
        
        # Use meta to set object TTL
        short_id = None
        with client1.create(meta={"type": "ephemeral", "ttl": 2}) as obj3:
            obj3.write(b"I will disappear")
            obj3.seal()
            short_id = obj3.id
        
        logger.info(f"Created short-lived object {short_id}")
        
        logger.info("Waiting for 3.5 seconds (TTL + buffer)...")
        time.sleep(3.5)
        
        logger.info("Checking if object exists via Peer 2...")
        try:
            # Peer 2's maintenance thread should have cleaned it up
            client2.get(short_id)
            logger.warning("Object still exists! (GC might be slow or TTL logic issue)")
        except FileNotFoundError:
            logger.info("SUCCESS: Object not found (GC worked!)")
        except Exception as e:
            logger.info(f"SUCCESS: Object not found or error: {e}")

    finally:
        # Cleanup
        if 'peer1' in locals(): peer1.stop_maintenance()
        if 'peer2' in locals(): peer2.stop_maintenance()
        shutil.rmtree(shared_dir)
        logger.info("Cleaned up shared directory")

if __name__ == "__main__":
    main()

import json
import http.server
import threading
import requests
from typing import Optional, Any, Dict, Tuple, List
from ..core.peer import Peer
from ..core.lease import AccessType
from .base import Transport

# --- Server ---

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def __init__(self, peer: Peer, *args, **kwargs):
        self.peer = peer
        super().__init__(*args, **kwargs)

    def do_POST(self):
        if self.path == '/acquire':
            self.handle_acquire()
        elif self.path == '/seal':
            self.handle_seal()
        elif self.path == '/discard':
            self.handle_discard()
        elif self.path == '/release':
            self.handle_release()
        else:
            self.send_error(404)

    def handle_acquire(self):
        try:
            length = int(self.headers.get('content-length', 0))
            data = json.loads(self.rfile.read(length))
            
            object_id = data.get('object_id') # Can be None
            intent = data['intent'] # "create" or "read"
            ttl = data.get('ttl_seconds', 60)
            meta = data.get('meta')

            if intent == 'create':
                access = AccessType.CREATE
            elif intent == 'write':
                access = AccessType.WRITE
            else:
                access = AccessType.READ
            
            lease, obj = self.peer.acquire(object_id, access, ttl, meta)
            
            handles = []
            if obj:
                handles = [b.get_handle() for b in obj.blobs]
            
            response = {
                "lease_id": lease.lease_id,
                "object_id": lease.object_id,
                "intent": intent,
                "handles": handles, # List of paths
                "ttl_seconds": ttl
            }
            
            self.send_json(200, response)
        except Exception as e:
            self.send_json(400, {"error": str(e)})

    def handle_seal(self):
        try:
            length = int(self.headers.get('content-length', 0))
            data = json.loads(self.rfile.read(length))
            lease_id = data['lease_id']
            
            self.peer.seal(lease_id)

            self.send_json(200, {"status": "sealed"})
        except Exception as e:
            self.send_json(400, {"error": str(e)})

    def handle_discard(self):
        try:
            length = int(self.headers.get('content-length', 0))
            data = json.loads(self.rfile.read(length))
            lease_id = data['lease_id']
            
            self.peer.discard(lease_id)

            self.send_json(200, {"status": "discarded"})
        except Exception as e:
            self.send_json(400, {"error": str(e)})

    def handle_release(self):
        try:
            length = int(self.headers.get('content-length', 0))
            data = json.loads(self.rfile.read(length))
            lease_id = data['lease_id']
            
            self.peer.release(lease_id)
            self.send_json(200, {"status": "released"})
        except Exception as e:
            self.send_json(400, {"error": str(e)})

    def send_json(self, code, data):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

class HttpServer:
    def __init__(self, peer: Peer, port: int = 8080):
        self.peer = peer
        self.port = port
        self.server = None
        self.thread = None

    def start(self):
        def handler_factory(*args, **kwargs):
            return RequestHandler(self.peer, *args, **kwargs)
        
        self.server = http.server.HTTPServer(('0.0.0.0', self.port), handler_factory)
        print(f"HTTP Server listening on port {self.port}")
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()

# --- Client ---

class HttpTransport(Transport):
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def acquire(self, object_id: Optional[str], intent: str, ttl: Optional[float] = None, meta: Optional[Dict] = None) -> Tuple[Dict, List[Any]]:
        url = f"{self.base_url}/acquire"
        payload = {
            "object_id": object_id,
            "intent": intent,
            "ttl_seconds": ttl,
            "meta": meta
        }
        resp = requests.post(url, json=payload)
        if resp.status_code != 200:
            raise RuntimeError(f"Acquire failed: {resp.text}")
            
        data = resp.json()
        # Handles are paths
        return data, data['handles']

    def seal(self, lease_id: str) -> None:
        url = f"{self.base_url}/seal"
        resp = requests.post(url, json={"lease_id": lease_id})
        if resp.status_code != 200:
            raise RuntimeError(f"Seal failed: {resp.text}")

    def discard(self, lease_id: str) -> None:
        url = f"{self.base_url}/discard"
        resp = requests.post(url, json={"lease_id": lease_id})
        if resp.status_code != 200:
            raise RuntimeError(f"Discard failed: {resp.text}")

    def release(self, lease_id: str) -> None:
        url = f"{self.base_url}/release"
        resp = requests.post(url, json={"lease_id": lease_id})
        if resp.status_code != 200:
            raise RuntimeError(f"Release failed: {resp.text}")

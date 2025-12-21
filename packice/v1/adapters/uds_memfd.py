from __future__ import annotations

import json
import os
import selectors
import socket
import struct
import sys
from types import SimpleNamespace
from typing import Dict

from packice.v1.adapters.base import ControlAdapter
from packice.v1.engine import AcquirePayload, LeasePayload, NodeState


def _recvall(sock: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("unexpected EOF")
        data.extend(chunk)
    return bytes(data)


class UDSMemfdAdapter(ControlAdapter):
    """Minimal UDS control plane with memfd-based attachment handles."""

    def __init__(self, state: NodeState, socket_path: str = "/tmp/packice.sock") -> None:
        self.state = state
        self.socket_path = socket_path
        self.sel = selectors.DefaultSelector()

    def serve(self) -> None:
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(self.socket_path)
        server.listen()
        server.setblocking(False)
        self.sel.register(server, selectors.EVENT_READ, data=None)
        try:
            while True:
                for key, _ in self.sel.select():
                    if key.data is None:
                        self._accept(key.fileobj)
                    else:
                        self._handle(key)
        finally:
            server.close()
            if os.path.exists(self.socket_path):
                os.unlink(self.socket_path)

    def _accept(self, sock: socket.socket) -> None:
        conn, _ = sock.accept()
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, data=SimpleNamespace(buffer=b""))

    def _handle(self, key: selectors.SelectorKey) -> None:
        sock: socket.socket = key.fileobj  # type: ignore[assignment]
        data: SimpleNamespace = key.data  # type: ignore[assignment]
        try:
            raw_len = sock.recv(4)
            if not raw_len:
                self.sel.unregister(sock)
                sock.close()
                return
            msg_len = struct.unpack("!I", raw_len)[0]
            payload = json.loads(_recvall(sock, msg_len))
            response, fd = self._dispatch(payload)
            out = json.dumps(response).encode()
            header = struct.pack("!I", len(out))
            if fd is None:
                sock.sendall(header + out)
            else:
                sock.sendmsg([header + out], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack("i", fd))])
                os.close(fd)
        except Exception as exc:
            err = json.dumps({"status": "error", "error": str(exc)}).encode()
            sock.sendall(struct.pack("!I", len(err)) + err)

    def _dispatch(self, payload: Dict):
        verb = payload.get("verb")
        if verb == "acquire":
            lease = self.state.acquire(AcquirePayload(**payload["body"]))
            fd = os.open(lease["attachment_path"], os.O_RDWR)
            return {"status": "ok", "lease": {**lease, "attachment_fd": True}}, fd
        if verb == "seal":
            return {"status": "ok", "lease": self.state.seal(LeasePayload(**payload["body"]))}, None
        if verb == "release":
            return {"status": "ok", **self.state.release(LeasePayload(**payload["body"]))}, None
        raise ValueError("unknown verb")


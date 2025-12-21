from __future__ import annotations

import os
from typing import Dict

from fastapi import FastAPI

from packice.v1.adapters.base import ControlAdapter
from packice.v1.engine import AcquirePayload, LeasePayload, NodeState


class HTTPFilesystemAdapter(ControlAdapter):
    """HTTP adapter backed by filesystem attachments (v0-compatible)."""

    def __init__(self, state: NodeState) -> None:
        self.state = state
        self.app = FastAPI()
        self._mount_routes()

    def _mount_routes(self) -> None:
        @self.app.post("/acquire")
        def acquire(req: AcquirePayload) -> Dict:
            return {"status": "ok", "lease": self.state.acquire(req)}

        @self.app.post("/seal")
        def seal(req: LeasePayload) -> Dict:
            return {"status": "ok", "lease": self.state.seal(req)}

        @self.app.post("/release")
        def release(req: LeasePayload) -> Dict:
            return {"status": "ok", **self.state.release(req)}

    def serve(self) -> None:
        import uvicorn

        uvicorn.run(
            self.app,
            host="0.0.0.0",
            port=int(os.getenv("PACKICE_PORT", "8080")),
        )


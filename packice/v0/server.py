import os
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


DEFAULT_TTL = int(os.getenv("PACKICE_TTL_SECONDS", "60"))
DATA_ROOT = Path(os.getenv("PACKICE_DATA", "/tmp/packice"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)


class AcquirePayload(BaseModel):
    objid: str
    intent: str
    ttl_seconds: Optional[int] = None
    meta: Optional[Dict] = None


class LeasePayload(BaseModel):
    lease_id: str


class NodeState:
    def __init__(self) -> None:
        self.objects: Dict[str, Dict] = {}
        self.leases: Dict[str, Dict] = {}
        self.attachments: Dict[str, Dict] = {}
        self.resolver: Dict[str, List[str]] = {}

    def _now(self) -> float:
        return time.time()

    def _active(self, lease: Dict) -> bool:
        return not lease.get("released") and self._now() <= lease["ttl"]

    def acquire(self, req: AcquirePayload) -> Dict:
        ttl = req.ttl_seconds or DEFAULT_TTL
        if req.intent not in {"create", "read"}:
            raise HTTPException(400, "intent must be create|read")
        for lid, lease in list(self.leases.items()):
            if not self._active(lease):
                self.release(LeasePayload(lease_id=lid))
        if req.intent == "create":
            if req.objid in self.objects and self.objects[req.objid]["state"] == "SEALED":
                raise HTTPException(400, "sealed copy exists")
            if any(
                l["objid"] == req.objid
                and l["intent"] == "create"
                and self._active(l)
                for l in self.leases.values()
            ):
                raise HTTPException(400, "active create lease exists")
            return self._grant_create(req, ttl)
        return self._grant_read(req.objid, ttl) or self._fetch_miss(req.objid, ttl)

    def _grant_create(self, req: AcquirePayload, ttl: int) -> Dict:
        attachment_id = str(uuid.uuid4())
        path = DATA_ROOT / f"{attachment_id}.bin"
        path.touch()
        lease_id = str(uuid.uuid4())
        self.leases[lease_id] = {
            "objid": req.objid,
            "intent": "create",
            "ttl": self._now() + ttl,
            "released": False,
            "sealed": False,
            "attachment_id": attachment_id,
        }
        self.objects[req.objid] = {
            "state": "CREATING",
            "meta": req.meta or {},
            "prev_objid": (req.meta or {}).get("prev_objid"),
            "attachment_id": attachment_id,
            "sealed_size": None,
        }
        self.attachments[attachment_id] = {
            "path": str(path),
            "sealed": False,
            "size": 0,
            "last": self._now(),
        }
        return self._lease_view(lease_id)

    def _grant_read(self, objid: str, ttl: int) -> Optional[Dict]:
        obj = self.objects.get(objid)
        if not obj or obj["state"] != "SEALED":
            return None
        attachment_id = obj["attachment_id"]
        lease_id = str(uuid.uuid4())
        self.leases[lease_id] = {
            "objid": objid,
            "intent": "read",
            "ttl": self._now() + ttl,
            "released": False,
            "sealed": True,
            "attachment_id": attachment_id,
        }
        self.attachments[attachment_id]["last"] = self._now()
        return self._lease_view(lease_id)

    def _fetch_miss(self, objid: str, ttl: int) -> Dict:
        for node in self.resolver.get(objid, []):
            try:
                import httpx

                remote = httpx.post(
                    f"{node}/acquire", json={"objid": objid, "intent": "read"}
                ).json()
                lease = remote["lease"]
                path = lease["attachment_path"]
                local = self._grant_create(AcquirePayload(objid=objid, intent="create"), DEFAULT_TTL)
                with open(path, "rb") as src, open(local["attachment_path"], "wb") as dst:
                    dst.write(src.read())
                self.seal(LeasePayload(lease_id=local["lease_id"]))
                httpx.post(f"{node}/release", json={"lease_id": lease["lease_id"]})
                return self._grant_read(objid, ttl) or {}
            except Exception:
                continue
        raise HTTPException(404, "object not found")

    def seal(self, req: LeasePayload) -> Dict:
        lease = self.leases.get(req.lease_id)
        if not lease or not self._active(lease) or lease["intent"] != "create" or lease["sealed"]:
            raise HTTPException(400, "lease not sealable")
        attachment = self.attachments[lease["attachment_id"]]
        size = Path(attachment["path"]).stat().st_size
        lease["sealed"] = True
        obj = self.objects[lease["objid"]]
        obj.update({"state": "SEALED", "sealed_size": size})
        attachment.update({"sealed": True, "size": size, "last": self._now()})
        self.resolver.setdefault(obj["objid"], []).append(
            os.getenv("PACKICE_PUBLIC_URL", "http://localhost")
        )
        return self._lease_view(req.lease_id)

    def release(self, req: LeasePayload) -> Dict:
        lease = self.leases.get(req.lease_id)
        if not lease:
            raise HTTPException(400, "unknown lease")
        lease["released"] = True
        if lease["intent"] == "create" and not lease["sealed"]:
            att = self.attachments.pop(lease["attachment_id"], None)
            if att:
                Path(att["path"]).unlink(missing_ok=True)
            self.objects.pop(lease["objid"], None)
        return {"released": req.lease_id}

    def _lease_view(self, lease_id: str) -> Dict:
        lease = self.leases[lease_id]
        obj = self.objects[lease["objid"]]
        att = self.attachments[lease["attachment_id"]]
        return {
            "lease_id": lease_id,
            "objid": lease["objid"],
            "intent": lease["intent"],
            "ttl_deadline": lease["ttl"],
            "sealed": lease["sealed"],
            "attachment_path": att["path"],
            "meta": obj["meta"],
            "sealed_size": obj["sealed_size"],
            "prev_objid": obj["prev_objid"],
        }


state = NodeState()
app = FastAPI()


@app.post("/acquire")
def acquire(req: AcquirePayload):
    return {"status": "ok", "lease": state.acquire(req)}


@app.post("/seal")
def seal(req: LeasePayload):
    return {"status": "ok", "lease": state.seal(req)}


@app.post("/release")
def release(req: LeasePayload):
    return {"status": "ok", **state.release(req)}


def run() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PACKICE_PORT", "8080")))


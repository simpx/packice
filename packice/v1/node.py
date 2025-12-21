from __future__ import annotations

import os
from typing import Literal

from packice.v1.adapters.http_fs import HTTPFilesystemAdapter
from packice.v1.adapters.uds_memfd import UDSMemfdAdapter
from packice.v1.engine import NodeState


def build_adapter(state: NodeState, adapter: Literal["http", "uds-memfd"]):
    if adapter == "uds-memfd":
        return UDSMemfdAdapter(state, socket_path=os.getenv("PACKICE_SOCKET", "/tmp/packice.sock"))
    return HTTPFilesystemAdapter(state)


def run() -> None:
    adapter = os.getenv("PACKICE_ADAPTER", "http")
    attachment_mode = "memfd" if adapter == "uds-memfd" else "fs"
    state = NodeState(attachment_mode=attachment_mode)
    build_adapter(state, adapter).serve()

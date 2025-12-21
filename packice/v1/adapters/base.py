from __future__ import annotations

import abc
from typing import Dict


class LeaseView(Dict):
    """Typed alias for lease responses."""


class ControlAdapter(abc.ABC):
    """Interface for binding PackIce core to a control transport."""

    @abc.abstractmethod
    def serve(self) -> None:
        """Start serving requests."""


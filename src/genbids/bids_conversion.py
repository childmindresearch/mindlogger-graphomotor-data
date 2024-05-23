"""Classes for data conversion to BIDS format."""

from typing import Protocol

from .bids_model import BidsModel


class Bidsifiable(Protocol):
    """BIDS data conversion class."""

    def bids_model(self) -> BidsModel:
        """Construct BIDS Model from current data model."""
        pass

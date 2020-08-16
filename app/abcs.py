import typing
from abc import ABC


class AbstractProcessingBackend(ABC):
    """A class implementing the protocols that extract and transform GEO data."""

    def process_item(self, *args, **kwargs) -> typing.Any:
        """The main processing pipeline for a single source URL."""
        pass

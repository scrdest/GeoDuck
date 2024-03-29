import os
import typing
from abc import ABC, abstractmethod


class AbstractProcessingBackend(ABC):
    """A class implementing the protocols that extract and transform GEO data."""

    @classmethod
    @abstractmethod
    def extract_item(cls, *args, **kwargs) -> typing.Any:
        """The main processing pipeline for a single source URL."""
        pass

    @classmethod
    @abstractmethod
    def save_extracted(cls, extracted, file: typing.Optional[os.PathLike] = None, *args, **kwargs) -> os.PathLike:
        """Write out the results of extract_item to a file."""


    @classmethod
    @abstractmethod
    def normalize_item(cls, extracted, *args, **kwargs) -> typing.Any:
        """Post-processing for the extracted data."""
        return extracted

    @classmethod
    @abstractmethod
    def save_normalized(cls, normalized, file: typing.Optional[os.PathLike] = None, *args, **kwargs) -> os.PathLike:
        """Write out the results of normalize_item to a file."""



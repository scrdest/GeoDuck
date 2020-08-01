import typing
from abc import ABC


class AbstractProcessingBackend(ABC):
    def process_item(self, *args, **kwargs) -> typing.Any:
        pass

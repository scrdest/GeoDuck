import typing

from abcs import AbstractProcessingBackend
from constants import DEFAULT_BACKEND_REGISTRY_KEY, BACKEND_LOCAL
from utils.registry import get_registry


def get_backend(backend_key, registry_key=None, *args, **kwargs) -> typing.Type[AbstractProcessingBackend]:
    from processing_backends import _registry_backend
    _repokey = registry_key or DEFAULT_BACKEND_REGISTRY_KEY
    parser = get_registry(_repokey)[backend_key]
    return parser


def process_item(
    backend: typing.Hashable = BACKEND_LOCAL,
    *args, **kwargs
):
    backend = get_backend(backend_key=backend)
    output = backend.process_item(*args, **kwargs)
    return output


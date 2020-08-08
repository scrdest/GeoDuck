import typing
from functools import wraps
from utils.misc import cache

from abcs import AbstractProcessingBackend
from constants import DEFAULT_BACKEND_REGISTRY_KEY, BACKEND_LOCAL, MAINARG_PROCESSING_BACKEND
from utils.registry import get_registry


@cache()
def get_backend(
    backend_key: typing.Hashable,
    registry_key=None,
    *args, **kwargs
) -> typing.Type[AbstractProcessingBackend]:

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


def with_backend(backend_key: typing.Hashable):

    def _injector_deco(mainfunc):

        @wraps(mainfunc)
        def _injector_wrapper(*raw_args, **raw_kwargs):
            injected_kwargs = raw_kwargs.copy()
            injected_kwargs[MAINARG_PROCESSING_BACKEND] = backend_key

            retval = mainfunc(*raw_args, **injected_kwargs)
            return retval

        return _injector_wrapper

    return _injector_deco

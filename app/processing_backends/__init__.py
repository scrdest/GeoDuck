import typing
from functools import wraps
from app.utils.misc import cache

from app.abcs import AbstractProcessingBackend
from app.constants import DEFAULT_BACKEND_REGISTRY_KEY, BACKEND_LOCAL, BACKEND_SPARK, MAINARG_PROCESSING_BACKEND
from app.utils.registry import get_registry


@cache()
def get_backend(
    backend_key: typing.Hashable,
    *args, **kwargs
) -> typing.Type[AbstractProcessingBackend]:
    """Returns a processing backend framework, e.g. Spark, Pandas...
    Fails if the queried key does not match a registered backend.

    :param backend_key: Hashable matching the key used to register the backend in the repository
    """
    # Simplified version of get_backend_fancy() - more maintenance overhead for adding new backends,
    # but far easier to debug in terms of existing ones because there's less indirection and no
    # Fancy Dynamic Loading (TM).
    from app.processing_backends.definitions import local as local_backend
    from app.processing_backends.definitions import spark as spark_backend

    backend_keymap = {
        BACKEND_LOCAL: local_backend.LocalProcessingBackend,
        BACKEND_SPARK: spark_backend.SparkProcessingBackend
    }

    parser = backend_keymap[backend_key]
    return parser


@cache()
def get_backend_fancy(
    backend_key: typing.Hashable,
    registry_key: typing.Optional[typing.Hashable] = None,
    *args, **kwargs
) -> typing.Type[AbstractProcessingBackend]:
    """Provides access to the Backend repository's items.
    Fails if the queried key does not match a registered backend.

    :param backend_key: Hashable matching the key used to register the backend in the repository
    :param registry_key: Optional; overrides the default registry to query for the backend
    """

    from app.processing_backends import _registry_backend
    _repokey = registry_key or DEFAULT_BACKEND_REGISTRY_KEY
    parser = get_registry(_repokey)[backend_key]
    return parser


def process_item(
    backend_key: typing.Hashable = BACKEND_LOCAL,
    *args, **kwargs
):
    backend = get_backend(backend_key=backend_key)
    output = backend.extract_item(*args, **kwargs)
    return output


def normalize_data(
    extracted: typing.Dict[str, tuple],
    backend_key: typing.Hashable = BACKEND_LOCAL,
    *args, **kwargs
):
    backend = get_backend(backend_key=backend_key)
    output = backend.normalize_item(extracted=extracted, *args, **kwargs)
    return output


def with_backend(backend_key: typing.Hashable):
    """A parameterized decorator that injects a specific `backend`
    parameter into the target function args at runtime.

    :param backend_key: Dict key mapping to a registered AbstractProcessingBackend implementation
    """

    def _injector_deco(mainfunc):

        @wraps(mainfunc)
        def _injector_wrapper(*raw_args, **raw_kwargs):
            injected_kwargs = raw_kwargs.copy()
            injected_kwargs[MAINARG_PROCESSING_BACKEND] = backend_key

            retval = mainfunc(*raw_args, **injected_kwargs)
            return retval

        return _injector_wrapper

    return _injector_deco

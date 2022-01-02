import os
import typing

from app.constants import DEFAULT_PARSER_REGISTRY_KEY, PARSER_GENERIC
from app.utils.registry import get_registry


fmt_map = {}


def get_parser(fmt_key, registry_key=None, *args, **kwargs):
    from app.parsers import _registry_backend
    _repokey = registry_key or DEFAULT_PARSER_REGISTRY_KEY
    registry = get_registry(_repokey)
    parser = (
        registry.get(fmt_key)
        or registry[PARSER_GENERIC]
    )
    return parser


def infer_format(
    filename: str,
    dataformat: typing.Optional[str] = None
) -> str:

    if dataformat:
        return dataformat

    splitname = filename.split(".", maxsplit=1)
    ext = None

    if len(splitname) == 2:
        basename, ext = splitname

    fmt = fmt_map.get(ext) or ext
    return fmt


def parse_format(
    data: str,
    dataformat: typing.Any,
    *args, **kwargs
):
    parser = get_parser(fmt_key=dataformat)
    output = parser(data, *args, **kwargs)
    return output


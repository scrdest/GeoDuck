import os
import typing

from constants import DEFAULT_PARSER_REGISTRY_KEY
from utils.registry import get_registry


fmt_map = {}


def get_parser(fmt_key, registry_key=None, *args, **kwargs):
    from parsers import _registry_backend
    _repokey = registry_key or DEFAULT_PARSER_REGISTRY_KEY
    parser = get_registry(_repokey)[fmt_key]
    return parser


def infer_format(
    filename: str,
    dataformat: typing.Optional[str] = None
) -> str:

    if dataformat: return dataformat
    basename, ext = os.path.splitext(filename)
    fmt = fmt_map.get(ext) or ext
    return fmt


def parse_format(
    data: str,
    dataformat: typing.Any,
    *args, **kwargs
):
    parser = get_parser(fmt_key=dataformat)
    output = parser.parse(data, *args, **kwargs)
    return output


import os
import typing

from parsers._registry_backend import parser_registry


def get_parser(dataformat, *args, **kwargs):
    parser = parser_registry[dataformat]
    return parser


def infer_format(filename: str, dataformat: typing.Optional[str] = None, *args, **kwargs):
    if dataformat: return dataformat
    basename, ext = os.path.splitext(filename)
    fmt = parser_registry[ext.lower()]
    return fmt


def parse_format(data: str, dataformat: typing.Any, *args, **kwargs):
    parser = get_parser(dataformat=dataformat)
    output = parser.parse(data, *args, **kwargs)
    return output

import app.constants as const
from app.utils.registry import registry_entry


@registry_entry(const.PARSER_GZIP, registry_key=const.DEFAULT_PARSER_REGISTRY_KEY)
@registry_entry(const.PARSER_GENERIC, registry_key=const.DEFAULT_PARSER_REGISTRY_KEY)
@registry_entry("", registry_key=const.DEFAULT_PARSER_REGISTRY_KEY)
def generic_parser(data, *args, **kwargs):
    return data

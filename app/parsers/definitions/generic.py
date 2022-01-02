import app.constants as const
from app.utils.logs import logger
from app.utils.registry import registry_entry
from app.utils.functional import tufilter


@registry_entry(const.PARSER_GZIP, registry_key=const.DEFAULT_PARSER_REGISTRY_KEY)
@registry_entry(const.PARSER_GENERIC, registry_key=const.DEFAULT_PARSER_REGISTRY_KEY)
@registry_entry("", registry_key=const.DEFAULT_PARSER_REGISTRY_KEY)
def generic_parser(data, *args, **kwargs):
    return data


@registry_entry("txt.gz", registry_key=const.DEFAULT_PARSER_REGISTRY_KEY)
def text_parser(data, *args, **kwargs):
    return "\n".join(tufilter(None, data.splitlines()))

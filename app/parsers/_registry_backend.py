import os
import app.constants as const
from app.constants import DEFAULT_PARSER_REGISTRY_KEY
from app.utils.registry import get_registry

DEFINITIONS_DIR_NAME = 'definitions'
PARSER_DIR = os.path.dirname(__file__)
DEFINITIONS_DIR = os.path.join(PARSER_DIR, DEFINITIONS_DIR_NAME)

_parser_registry = get_registry(
    registry_key=DEFAULT_PARSER_REGISTRY_KEY,
    raise_on_missing=False
)

if not _parser_registry:
    from app.utils.registry.autodiscovery import autodiscover

    pkg_prefix = '.'.join(
        os.path.split(
            os.path.relpath(
                PARSER_DIR,
                const.BASE_DIR
            )
        )
    )

    autodiscover(
        registry=_parser_registry,
        discovery_dir_name=DEFINITIONS_DIR_NAME,
        root=PARSER_DIR,
        pkg_prefix=pkg_prefix
    )()

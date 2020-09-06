import os
import constants as const
from app.utils.registry import get_registry

DEFINITIONS_DIR_NAME = 'definitions'
INTERFACE_DIR = os.path.dirname(__file__)
DEFINITIONS_DIR = os.path.join(INTERFACE_DIR, DEFINITIONS_DIR_NAME)

_interface_registry = get_registry(
    registry_key=const.DEFAULT_PARSER_REGISTRY_KEY,
    raise_on_missing=False
)

if not _interface_registry:
    from app.utils.registry.autodiscovery import autodiscover

    pkg_prefix = '.'.join(
        os.path.split(
            os.path.relpath(
                INTERFACE_DIR,
                const.BASE_DIR
            )
        )
    )
    print(f"PKG PREFIX is `{pkg_prefix}`")

    autodiscover(
        registry=_interface_registry,
        discovery_dir_name=DEFINITIONS_DIR_NAME,
        root=INTERFACE_DIR,
        pkg_prefix=pkg_prefix
    )()

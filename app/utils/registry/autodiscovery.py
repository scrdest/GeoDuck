import glob
import importlib
import os


def autodiscover(
    registry,
    pkg_prefix,
    discovery_dir_name,
    root=None,
    logger=None
):

    ran = registry and len(registry) > 0

    def _autodiscover():
        _discovery_root = '.' if root is None else root
        _discovery_fulldir = os.path.join(
            _discovery_root,
            discovery_dir_name
        )

        discovery_pattern = os.path.join(
            _discovery_fulldir,
            '*.py'
        )

        module_count = 0

        for source_path in glob.iglob(discovery_pattern):
            raw_source_module = os.path.splitext(source_path)[0]
            source_module = os.path.relpath(raw_source_module, _discovery_fulldir)
            module_name = f'{pkg_prefix}.{discovery_dir_name}.{source_module}'
            importlib.import_module(
                name=module_name,
                package=pkg_prefix
            )
            module_count += 1

        if logger:
            logger.info(
                'Autodiscovery: {entry_count} entries discovered in {file_count} modules'
                .format(
                    entry_count=len(registry),
                    file_count=module_count
                )
            )
        return registry


    def _placeholder():
        return registry

    return _placeholder if ran else _autodiscover

import os

import hydra

import constants
from core.app import main

from interface import get_interface
from processing_backends import with_backend

config_file = os.environ.get(constants.ENV_CONFIGNAME_KEY, constants.DEFAULT_CONFIG_FILENAME)


@hydra.main(config_path=config_file, strict=False)
def run(cfg=None):
    """Entrypoint. Wraps the main method with an interface (e.g. CLI or GUI),
    based on a bootstrap CLI argument for interface type (defaults to CLI).
    """
    target_interface_key = cfg.interface if cfg else None
    interface = get_interface(target_interface_key)

    target_backend_key = cfg.backend if cfg else constants.BACKEND_SPARK
    backend = with_backend(target_backend_key)
    wrapped_main = interface(
        backend(
            main
        )
    )
    status = wrapped_main(cfg=cfg)
    return status


if __name__ == '__main__':
    run()

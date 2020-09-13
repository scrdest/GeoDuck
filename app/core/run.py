import os

import app.constants as constants

from app.core.mainloop import main
from app.interface.core import get_interface
from app.processing_backends import with_backend


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

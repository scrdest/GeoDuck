import glob
import os
from app.interface.definitions.invoke_cli import *
from app import constants

if __name__ == '__main__':
    from invoke import Context
    ctx = Context()
    config_path = os.path.join(
        constants.CONFIG_DIR,
        "config.yaml"
    )

    run(
        ctx,
        config_path=config_path
    )

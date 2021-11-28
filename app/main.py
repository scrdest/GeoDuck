import os
import sys

import omegaconf

import app.constants as constants

if constants.BASE_DIR not in sys.path:
    sys.path.append(constants.BASE_DIR)

from app.core.run import run


def entrypoint():
    config_file = (
        os.environ.get(constants.ENV_CONFIGNAME_KEY)
        or os.path.join(
            constants.DEFAULT_CONFIG_DIR,
            constants.DEFAULT_CONFIG_FILENAME
        )
    )
    config = omegaconf.OmegaConf.load(config_file)
    sys.exit(run(cfg=config))


if __name__ == '__main__':
    entrypoint()

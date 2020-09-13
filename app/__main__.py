import os
import sys

import hydra

import app.constants as constants

if constants.BASE_DIR not in sys.path:
    sys.path.append(constants.BASE_DIR)

from app.core.run import run

config_file = os.environ.get(constants.ENV_CONFIGNAME_KEY, constants.DEFAULT_CONFIG_FILENAME)
run_with_hydra = hydra.main(config_path=config_file, strict=False)(run)
run_with_hydra()

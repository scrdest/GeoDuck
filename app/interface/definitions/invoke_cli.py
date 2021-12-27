import os

import app.constants as const

from app.core import mainloop
from app.utils.registry import registry_entry
from app.utils.logs import logger

VERSION_FILEPATH = os.path.join(const.BASE_DIR, "version.txt")
INVOKE_AVAILABLE = True

try:
    import invoke
except ImportError as IErr:
    logger.exception(IErr)
    INVOKE_AVAILABLE = False


def get_version():
    version = None

    with open(VERSION_FILEPATH) as version_file:
        for line in version_file:
            version = line.strip()
            if version:
                break

    return version


@invoke.task()
def run(c, config_path=None):
    """Run the full program, with exact behaviour defined by an (optional) config file."""
    run_kwargs = dict()
    config = None

    if config_path:
        import omegaconf

        if os.path.sep in config_path:
            # If there are any separators, assume it's a full absolute or relative path
            norm_config_path = os.path.abspath(config_path)
        else:
            # No separators - assume it's a filename in the default directory
            norm_config_path = os.path.join(const.DEFAULT_CONFIG_DIR, config_path)

        config = omegaconf.OmegaConf.load(norm_config_path)

    loop = mainloop.coreloop(cfg=config, **run_kwargs)
    for savepath in loop:
        logger.info(f"Output: {savepath}")


@invoke.task()
def fetch_raw(c):
    """Search NCBI GEO, download raw data files via FTP and save locally."""
    run_kwargs = dict()
    run_kwargs[const.MAINARG_SAVE_DOWNLOADED] = True

    loop = mainloop.coreloop(**run_kwargs)
    for savepath in loop:
        logger.info(f"Savepath: {savepath}")


@invoke.task()
def fetch_normalized(c):
    """Search NCBI GEO, process and save normalized data files."""
    run_kwargs = dict()
    run_kwargs[const.MAINARG_SAVE_NORMALIZED] = True

    loop = mainloop.coreloop(**run_kwargs)
    for savepath in loop:
        logger.info(f"Savepath: {savepath}")


def build_namespaces():
    base_namespace = invoke.Collection()

    fetch_namespace = invoke.Collection()
    fetch_namespace.add_task(fetch_raw, name="raw")
    fetch_namespace.add_task(fetch_normalized, name="normalized")

    base_namespace.add_task(run, name="run")
    base_namespace.add_collection(fetch_namespace, name="fetch")
    return base_namespace


@registry_entry(const.INTERFACE_INVOKE, registry_key=const.DEFAULT_INTERFACE_REGISTRY_KEY)
def run_program(*args, **kwargs):
    if INVOKE_AVAILABLE:
        program = invoke.Program(
            namespace=build_namespaces(),
            version=get_version() or None
        )
        program.run()
        return program

    elif os.environ.get(const.ENV_DEFAULT_TO_BASIC_CLI, "").lower().strip() == "true":
        # if GDUCK_USE_CLI_FALLBACK is True, use a simple entrypoint as a fallback
        from app.main import entrypoint
        entrypoint()

    else:
        raise RuntimeError("\n".join((
            "This entrypoint requires the Invoke lib to be installed.",
            "(you can disable this warning and fall back to a default entrypoint ",
            f"instead by setting the {const.ENV_DEFAULT_TO_BASIC_CLI} envvar to True)"
        )))


if __name__ == '__main__':
    run_program()

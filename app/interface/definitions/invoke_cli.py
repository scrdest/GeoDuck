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
def run(c):
    pass


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

    elif os.environ.get(const.ENV_DEFAULT_TO_BASIC_CLI):
        # if GDUCK_USE_CLI_FALLBACK is True, use a simple entrypoint as a fallback
        from app.main import entrypoint
        entrypoint()

    else:
        raise RuntimeError("This entrypoint requires the Invoke lib to be installed.")


if __name__ == '__main__':
    run_program()






import argparse
import typing

import app.constants as const
from app.constants import DEFAULT_INTERFACE_REGISTRY_KEY
from app.utils.registry import get_registry


def build_bootstrap_parser() -> argparse.ArgumentParser:
    """Defines and returns a simple parser for the arguments specifying the app interface."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-i', '--interface',
        type=str,
        nargs='?',
        required=False,
        default=const.INTERFACE_CLI,
        dest=const.ARG_INTERFACE
    )

    return parser


def read_bootstrap_args() -> typing.Tuple[dict, list]:
    """Creates and runs a simple parser for the arguments specifying the app interface.

    Returns a tuple of known arguments (as dict) and unknown arguments (as list).
    """
    cli_parser = build_bootstrap_parser()
    known, unknown = cli_parser.parse_known_args()
    return vars(known), unknown


def get_interface(
    interface_key: typing.Optional[typing.Hashable] = None,
    registry_key: typing.Optional[typing.Hashable] = None,
    *args, **kwargs
) -> typing.Callable:
    """Fetches an app UI based on the arguments passed to the minimalistic bootstrap CLI
    and instantiates it with any arguments not consumed by the bootstrapper interface.

    Returns a wrapper for the function which receives the arguments from the UI.
    """
    _interface_key = interface_key
    other_args = {}
    if not _interface_key:
        bootstrap_args, other_args = read_bootstrap_args()
        _interface_key = interface_key or bootstrap_args.get(const.ARG_INTERFACE) or const.INTERFACE_NONE

    from app.interface import _registry_backend
    _repokey = registry_key or DEFAULT_INTERFACE_REGISTRY_KEY

    interface = get_registry(_repokey)[_interface_key]
    interface_wrapper = interface(other_args, *args, **kwargs)
    return interface_wrapper

import typing

from constants import DEFAULT_INTERFACE_REGISTRY_KEY
from utils.registry import get_registry

import argparse
import constants as const


def build_bootstrap_parser():
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


def read_bootstrap_args():
    cli_parser = build_bootstrap_parser()
    known, unknown = cli_parser.parse_known_args()
    return vars(known), unknown



def get_interface(registry_key=None, *args, **kwargs):
    bootstrap_args, other_args = read_bootstrap_args()
    interface_key = bootstrap_args.get(const.ARG_INTERFACE) or const.INTERFACE_CLI

    from interface import _registry_backend
    _repokey = registry_key or DEFAULT_INTERFACE_REGISTRY_KEY

    interface = get_registry(_repokey)[interface_key]
    interface_wrapper = interface(other_args, *args, **kwargs)
    return interface_wrapper

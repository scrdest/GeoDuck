import argparse
from functools import wraps

import constants as const
from utils.registry import registry_entry


def build_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-q', '--query',
        type=str,
        nargs='*',
        dest=const.ARG_QUERY
    )

    parser.add_argument(
        '-o', '--organism',
        type=str,
        nargs='?',
        dest=const.ARG_ORGANISM
    )

    return parser
    
    
def read_args(parser=None, preloaded_args=None):
    cli_parser = parser or build_parser()
    known, unknown = cli_parser.parse_known_args(preloaded_args)
    return vars(known)


@registry_entry(const.INTERFACE_CLI, registry_key=const.DEFAULT_INTERFACE_REGISTRY_KEY)
def cli_parser(cli_args, *args, **kwargs):

    def _cli_deco(func):

        @wraps(func)
        def _cli_wrapper(*fargs, **fkwargs):
            amended_kwargs = fkwargs.copy()
            cli_args = read_args(parser=None)
            amended_kwargs.update(cli_args)

            result = func(*fargs, **amended_kwargs)
            return result

        return _cli_wrapper

    return _cli_deco

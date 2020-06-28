import argparse

import constants as const

def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-q', '--query',
        type=str,
        nargs='*',
        dest=const.ARG_QUERY
    )
    return parser
    
    
def read_args(parser=None):
    cli_parser = parser or build_parser()
    known, unknown = cli_parser.parse_known_args()
    return vars(known)

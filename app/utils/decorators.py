import logging
import os
import sys
import typing
from functools import wraps
from pprint import pformat

try: import constants as const
except ImportError: import app.constants as const


def preflight_message(
    msg: str,
    disabled: bool = False,
    to_stream: typing.Optional[typing.TextIO] = None
) -> typing.Callable:

    """Parameterized decorator; if enabled, logs a specified message to
    a stream (stdout by default) before executing the decorated function.

    :param msg: Message to write out before each call to the decorated function.
    :param disabled: If True, the target function is not decorated but is returned as-is.
    :param to_stream: Overrides the output stream from stdout to a custom sink.
    """

    def _preflight_msg_deco(func):

        @wraps(func)
        def _preflightwrapper(*args, **kwargs):
            print(msg, file=to_stream)
            return func(*args, **kwargs)

        return func if disabled else _preflightwrapper

    return _preflight_msg_deco


def with_print(pretty: bool = False, disabled: bool = False, to_stream: typing.Optional[typing.TextIO] = None):
    """Parameterized decorator; debugging helper - if enabled,
    (pretty- or regular-) prints the decorated function's return
    value to a stream (stdout by default).

    :param pretty: If True, uses pprint() to format the output, otherwise does a plain old print() [faster, less nice].
    :param disabled: If True, the target function is not decorated but is returned as-is.
    :param to_stream: Overrides the output stream from stdout to a custom sink.
    """
    def _with_print_deco(func):
    
        @wraps(func)
        def _printwrapper(*args, **kwargs):
            printer = lambda x, s, **pkwargs: print(x, file=(s or sys.stdout), **pkwargs)
            print_kwargs = dict()
            
            if pretty:
                from pprint import pprint
                printer = lambda x, s, **pkwargs: pprint(x, stream=(s or sys.stdout), **pkwargs)
                
            result = func(*args, **kwargs)
            
            if isinstance(to_stream, str):
                with open(to_stream, ('w')) as _fileout:
                    printer(result, _fileout)
            else:
                printer(result, to_stream)
                
            return result
            
        return func if disabled else _printwrapper
        
    return _with_print_deco


def with_logging(
    pretty: bool = False,
    disabled: bool = False,
    level: typing.Optional[int] = None,
    logger_name: typing.Optional[str] = None,
    message_prefix: str = "",
    **logger_kwargs,
):
    """Parameterized decorator; debugging helper - if enabled,
    (pretty- or regular-) prints the decorated function's return
    value to the logs.

    :param pretty: If True, uses pprint() to format the output, otherwise does a plain old print() [faster, less nice].
    :param disabled: If True, the target function is not decorated but is returned as-is.
    :param level: log level to use, as a logging level constant int; INFO by default.
    :param logger_name: logger to use (as identified by a logger name). Uses app logger by default.
    :param message_prefix: optional string to be prepended before the return value
    :param logger_kwargs: passes on parameters to the logger builder
    """
    def _with_logging_deco(func):

        @wraps(func)
        def _logwrapper(*args, **kwargs):
            from app.utils.logs import get_logger

            logger = get_logger(
                name=logger_name,
                level=level,
                **logger_kwargs
            )

            result = func(*args, **kwargs)
            formatted_result = pformat(result, indent=4) if pretty else result
            prefixed_output = f"{message_prefix}{formatted_result}"

            logger.log(level=level or logging.INFO, msg=prefixed_output)

            return result

        return func if disabled else _logwrapper

    return _with_logging_deco


def result_to_json(filename: typing.Optional[str], disabled: bool = False):
    """Parameterized decorator that dumps the output of the decorated
    function to a JSON file.

    :param filename: Filesystem path for the JSON file to dump the data to.
    :param disabled: If True, the target function is not decorated but is returned as-is.
    """

    def _jsondump_deco(func):
    
        @wraps(func)
        def _resultwrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            
            import json
            _filename = filename or os.path.join(const.OUTPUT_DIR, 'result.json')
            with open(_filename, 'w') as jsonfile:
                json.dump(result, jsonfile)
                
            return result
            
        return func if disabled else _resultwrapper
        
    return _jsondump_deco

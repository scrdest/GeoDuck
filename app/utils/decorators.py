import datetime
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


def capture_bad_inputs(func=None, to_dir: os.PathLike = None, use_pickle=True, capture_error=False, enabled=None):
    """Parametrizeable decorator. If the decorated function throws an exception,
    serializes the function parameters to a specified directory.

    :param to_dir: Base directory to write the failed function's parameters to.
    :param use_pickle: Serializer switch:
                       True for Python Pickle (more flexible, less safe) - DEFAULT
                       False for JSON (safer, constrained to data only)
    :param capture_error: If True, captures the exception that caused the error. False (default) - just arguments.
    :param enabled: If False, does not apply the decorator. If None (default), looks up value from an envvar.
    :return: Decorator, to be applied on a function
            (use either as a parameterized decorator, e.g. "@capture_bad_inputs()"
            or just as a plain "@capture_bad_inputs" to use defaults values for args)
    """

    _disabled = (
        enabled if enabled is not None
        else (
            (os.environ.get(const.ENV_CAPTURE_BAD_INPUTS_ENABLED) or "").lower()
            == "true"
        )
    )
    _to_dir = to_dir or const.DEFAULT_BAD_ARGS_DIR

    def _catcherdeco(_func):

        @wraps(_func)
        def _catcherwrapper(*args, **kwargs):
            fmt_suffix = ""
            try:
                result = _func(*args, **kwargs)

            except Exception as E:
                logging.debug(f"Capturing exception args for: {E}", stack_info=True)

                if use_pickle:
                    from pickle import dump as serialize
                    filemode = "wb"
                    fmt_suffix = ".pkl"

                else:
                    from json import dump as serialize
                    filemode = "w"
                    fmt_suffix = ".json"

                dump_fname = f"{datetime.datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_{_func.__name__}-FAILEDARGS{fmt_suffix}"
                os.makedirs(_to_dir, exist_ok=True)
                to_loc = os.path.join(_to_dir, dump_fname)
                data_to_serialize = (args, kwargs)
                if capture_error:
                    data_to_serialize += (E,)

                with open(to_loc, filemode) as dumpfile:
                    serialize(data_to_serialize, dumpfile)

                raise

            return result

        return _func if _disabled else _catcherwrapper

    if func:
        return _catcherdeco(func)

    return _catcherdeco

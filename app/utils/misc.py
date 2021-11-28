import os
from functools import wraps

import app.constants as const
from app.utils.logs import logger


def cache(disabled=False):

    def _cache_deco(func):
        if disabled:
            return func

        results = {}

        @wraps(func)
        def _cache_wrapper(*args, **kwargs):
            frozen_kwargs = tuple(kwargs.items())
            cache_key = (args, frozen_kwargs)

            try:
                result = results[cache_key]
            except KeyError:
                result = func(*args, **kwargs)
                results[cache_key] = result

            return result

        return _cache_wrapper

    return _cache_deco


def to_jsonl(iterable, base_filename=None):
    import json
    _base_filename = base_filename or os.path.join(const.OUTPUT_DIR, 'result')

    files = {}
    try:
        for (idx, item) in enumerate(iterable):
            logger(idx)
            filename = '{base}_{num}.json'.format(
                base=_base_filename,
                num=(idx // 1000)
            )
            fd = files.setdefault(filename, open(filename, 'w'))
            fd.write(json.dumps(item))
            fd.write('\n')
    finally:
        for fd in files.values():
            fd.close()
    return True

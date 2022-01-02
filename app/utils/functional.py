import functools
import typing
import app.constants

REIFY_KEYWORD = "reify"


def reifiable(func: typing.Callable) -> typing.Callable:
    """Decorator. Transform the functions that return lazy iterators
    into functions that *optionally* return tuples.

    This is controlled by injecting a new 'reify' boolean flag as a function
    parameter; if True, the result will be a tuple, otherwise - the underlying
    lazy iterable.
    """

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        reify = kwargs.pop(REIFY_KEYWORD) if REIFY_KEYWORD in kwargs else False
        raw_result = func(*args, **kwargs)
        result = tuple(raw_result) if reify else raw_result
        return result

    return _wrapper


# reifiable variants of basic FP functions:
tumap = reifiable(map)
tufilter = reifiable(filter)


def batch(iterable, batch_size=None):
    _batch_size = batch_size or app.constants.DEFAULT_BATCH_SIZE

    if _batch_size < 1:
        raise ValueError(f"Batch size must be positive! (Found: {batch_size})")

    iterator = iter(iterable)

    while True:
        batch = []
        try:
            for i in range(_batch_size):
                nextitem = next(iterator)
                batch.append(nextitem)
            yield batch

        except StopIteration:
            yield batch
            break

    return

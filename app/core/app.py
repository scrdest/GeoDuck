import time

import constants as const
from core.fetch.fetching import fetch_all
from core.search import NcbiDbs
from processing_backends import with_backend, process_item


def parse_app_args(app_args: dict) -> dict:
    """Cleans, infers, defaults, and translates the
    app arguments for main() from raw UI args.

    :param app_args: Raw parameters from the UI, as a dict.
    """
    results = dict()

    free_text_query = '+AND+'.join((
        app_args.get(const.MAINARG_QUERY)
        or []
    ))

    organism_query = '{species}[Organism]'.format(
        species=(
                app_args.get(const.ARG_ORGANISM)
                or 'human'
        )
    )

    raw_terms = [
        free_text_query,
        organism_query,
        'gsm[EntryType]',
        'csv[Supplementary Files]'
    ]
    qry_term = '+AND+'.join(filter(None, raw_terms))

    increment = app_args.get(const.MAINARG_INCREMENT, const.DEFAULT_SEARCH_INCREMENT)
    batch_size = const.DEFAULT_SEARCH_INCREMENT if increment is None else max(increment, 1)

    results[const.MAINARG_DATABASE] = (app_args.get(const.ARG_DATABASE) or NcbiDbs.GDS.value)
    results[const.MAINARG_QUERY] = qry_term
    results[const.MAINARG_BATCH_SIZE] = batch_size
    results[const.MAINARG_PROCESSING_BACKEND] = app_args.get(const.MAINARG_PROCESSING_BACKEND) or const.BACKEND_LOCAL

    return results


@with_backend(const.BACKEND_SPARK)
def main(**kwargs):
    """Root of the pipeline.

    Purely programmatic - human-friendly UIs can slot into it by creating wrappers
    that inject kwargs into this function.

    All supported parameter keys are constants with the `MAINARG_` prefix.
    """
    app_args = parse_app_args(kwargs)
    backend = app_args[const.MAINARG_PROCESSING_BACKEND]
    batch = 'not started'

    precalculated_sources = app_args.get(const.MAINARG_PRECALCULATED_SOURCES)

    fetcher = None
    if precalculated_sources:
        # User knows what they want and gave us a list of sources,
        # possibly from a cached/manual search - run with it.
        fetcher = (src for src in precalculated_sources)
    else:
        # Run the search to get the links.
        db = app_args[const.MAINARG_DATABASE]
        term = app_args[const.MAINARG_QUERY]
        batch_size = app_args[const.MAINARG_BATCH_SIZE]

        fetcher = fetch_all(term=term, db=db, batch_size=batch_size)

    while batch:
        batch = next(fetcher, None)
        import random
        randaddr, randfile = random.choice(tuple(batch.values()))
        result = process_item(
            backend=backend,
            addr=randaddr,
            fname=randfile
        )
        time.sleep(1)

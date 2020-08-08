import time

import constants as const
from core.fetch.fetching import fetch_all
from core.search import NcbiDbs
from processing_backends import with_backend, process_item


def parse_app_args(app_args: dict) -> dict:
    results = dict()

    free_text_query = '+AND+'.join((
        app_args.get(const.ARG_QUERY)
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
    results[const.ARG_QUERY] = qry_term
    results[const.MAINARG_BATCH_SIZE] = batch_size
    results[const.MAINARG_PROCESSING_BACKEND] = app_args.get(const.MAINARG_PROCESSING_BACKEND) or const.BACKEND_LOCAL

    return results


@with_backend(const.BACKEND_SPARK)
def main(**kwargs):
    app_args = parse_app_args(kwargs)

    db = app_args[const.MAINARG_DATABASE]
    term = app_args[const.ARG_QUERY]
    batch_size = app_args[const.MAINARG_BATCH_SIZE]
    backend = app_args[const.MAINARG_PROCESSING_BACKEND]

    fetcher = fetch_all(term=term, db=db, batch_size=batch_size)
    batch = 'not started'

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

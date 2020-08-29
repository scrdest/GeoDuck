import time

import constants as const
from core.fetch.fetching import fetch_all
from core.search import NcbiDbs
from processing_backends import with_backend, process_item


def parse_app_args(cfg: object = None, app_args: dict = None) -> dict:
    """Cleans, infers, defaults, and translates the
    app arguments for main() from raw UI args.

    :param cfg: Configuration instance
    :param app_args: Raw parameters from the UI, as a dict.
    """
    _app_args = app_args or dict()
    results = dict()

    free_text_query = (
        '+AND+'.join((
            _app_args.get(const.MAINARG_QUERY)
            or []
        ))
        or cfg.query.text  # deliberately OUTSIDE of the join() to allow more customization
    )


    species = (
        _app_args.get(const.ARG_ORGANISM)
        or cfg.query.organism
    )
    organism_query = '{species}[Organism]'.format(species=species) if species else None


    entrytype = (
        None  # TODO: add CLI arg for it
        or cfg.query.entrytype
    )
    entrytype_query = '{entrytype}[EntryType]'.format(entrytype=entrytype) if entrytype else None


    fileformat = (
            None  # TODO: add CLI arg for it
            or cfg.query.fileformat
            or 'csv'
    )
    fileformat_query = '{fileformat}[Supplementary Files]'.format(fileformat=fileformat) if fileformat else None


    raw_terms = [
        free_text_query,
        organism_query,
        entrytype_query,
        fileformat_query
    ]

    qry_term = '+AND+'.join(filter(None, raw_terms))

    increment = _app_args.get(const.MAINARG_INCREMENT) or cfg.batch_size or const.DEFAULT_SEARCH_INCREMENT
    batch_size = const.DEFAULT_SEARCH_INCREMENT if increment is None else max(increment, 1)

    results[const.MAINARG_DATABASE] = (_app_args.get(const.ARG_DATABASE) or NcbiDbs.GDS.value)
    results[const.MAINARG_QUERY] = qry_term
    results[const.MAINARG_BATCH_SIZE] = batch_size
    results[const.MAINARG_PROCESSING_BACKEND] = (
        _app_args.get(const.MAINARG_PROCESSING_BACKEND)
        or cfg.backend
        or const.BACKEND_LOCAL
    )
    results[const.MAINARG_PRECALCULATED_SOURCES] = dict(cfg.accession_numbers) if cfg.accession_numbers else None

    return results


def main(cfg=None, **kwargs):
    """Root of the pipeline.

    Purely programmatic - human-friendly UIs can slot into it by creating wrappers
    that inject kwargs into this function.

    All supported parameter keys are constants with the `MAINARG_` prefix.
    """
    app_args = parse_app_args(cfg=cfg, app_args=kwargs)
    backend = app_args[const.MAINARG_PROCESSING_BACKEND]
    batch = 'not started'

    precalculated_sources = app_args.get(const.MAINARG_PRECALCULATED_SOURCES)

    fetcher = None
    if precalculated_sources:
        # User knows what they want and gave us a list of sources,
        # possibly from a cached/manual search - run with it.
        fetcher = ({k: v} for k, v in precalculated_sources.items())
    else:
        # Run the search to get the links.
        db = app_args[const.MAINARG_DATABASE]
        term = app_args[const.MAINARG_QUERY]
        batch_size = app_args[const.MAINARG_BATCH_SIZE]

        fetcher = fetch_all(term=term, db=db, batch_size=batch_size)

    while batch:
        batch = next(fetcher, None)
        if not batch: continue
        import random
        randaddr, randfile = random.choice(tuple(batch.values()))
        result = process_item(
            backend=backend,
            addr=randaddr,
            fname=randfile
        )
        time.sleep(1)

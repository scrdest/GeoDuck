import typing
import app.constants as const
from app.core.fetch.fetching import fetch_all
from app.processing_backends import get_backend


def parse_app_args(cfg: object = None, ui_args: dict = None) -> dict:
    """Cleans, infers, defaults, and translates the app arguments for main() from raw UI args.
    A UI -> core adapter, in other words.

    :param cfg: Configuration instance
    :param app_args: Raw parameters from the UI, as a dict.
    """
    _app_args = ui_args or dict()

    # Any required keywords (ANDed together!) in the free text search over all categories
    free_text_query = (
        '+AND+'.join((
            _app_args.get(const.MAINARG_QUERY)
            or []
        ))
        or (
            # deliberately OUTSIDE of the join() to allow more customization
            cfg.query.text
            if cfg and cfg.query
            else None
        )
    )

    # Species (e.g. Homo Sapiens or S. Cerevisiae)
    species = (
        _app_args.get(const.MAINARG_ORGANISM)
        or (
            cfg.query.organism
            if cfg and cfg.query
            else None
        )
    )
    species_query = '{species}[Organism]'.format(species=species) if species else None

    # Entry type (DataSet, Series, Samples, Platforms...)
    entrytype = (
        None  # TODO: add CLI arg for it
        or (
            cfg.query.entrytype
            if cfg and cfg.query
            else None
        )
    )
    entrytype_query = '{entrytype}[EntryType]'.format(entrytype=entrytype) if entrytype else None

    # Supplementary file format (e.g. CEL, GPR, WIG... - to filter down to only what we can parse)
    fileformat = (
            None  # TODO: add CLI arg for it
            or (
                cfg.query.fileformat
                if cfg and cfg.query
                else None
            )
            or 'csv'
    )
    fileformat_query = '{fileformat}[Supplementary Files]'.format(fileformat=fileformat) if fileformat else None

    # Assemble the search terms:
    raw_terms = [
        free_text_query,
        species_query,
        entrytype_query,
        fileformat_query
    ]

    qry_term = '+AND+'.join(filter(None, raw_terms))

    # Customizing the search formatting
    increment = (
        _app_args.get(const.MAINARG_INCREMENT)
        or (cfg.batch_size if cfg else None)
        or const.DEFAULT_SEARCH_INCREMENT
    )
    batch_size = const.DEFAULT_SEARCH_INCREMENT if increment is None else max(increment, 1)

    # Populating an output dict with standardized keys and defaulted values
    # This could probably be reworked into some object, but for now a dict does the trick
    results = dict()
    results[const.MAINARG_DATABASE] = (_app_args.get(const.MAINARG_DATABASE) or const.DEFAULT_DB_VALUE)
    results[const.MAINARG_QUERY] = qry_term
    results[const.MAINARG_BATCH_SIZE] = batch_size
    results[const.MAINARG_PROCESSING_BACKEND] = (
        _app_args.get(const.MAINARG_PROCESSING_BACKEND)
        or (cfg.backend if cfg else None)
        or const.BACKEND_LOCAL
    )
    results[const.MAINARG_PRECALCULATED_SOURCES] = dict(cfg.accession_numbers) if (cfg and cfg.accession_numbers) else None
    results[const.MAINARG_DRY_RUN] = bool(cfg.dry_run) if (cfg and cfg.dry_run) else False

    return results


def get_fetcher(app_args: dict) -> typing.Iterable[typing.Mapping]:
    fetcher = None  # null object

    precalculated_sources = app_args.get(const.MAINARG_PRECALCULATED_SOURCES)

    if precalculated_sources:
        # User knows what they want and gave us a list of sources,
        # possibly from a cached/manual search - run with it.
        fetcher = ({k: v} for k, v in precalculated_sources.items())

    else:
        # Run the search to get the links
        # (or rather, create a cursor-ey iterator over the search)
        db = app_args[const.MAINARG_DATABASE]
        term = app_args[const.MAINARG_QUERY]
        batch_size = app_args[const.MAINARG_BATCH_SIZE]

        fetcher = fetch_all(term=term, db=db, batch_size=batch_size)

    return fetcher


def main(cfg=None, **kwargs):
    """Root of the pipeline.

    Purely programmatic - human-friendly UIs can slot into it by creating adapters
    that inject kwargs into this function.

    All supported parameter keys are constants with the `MAINARG_` prefix.
    """
    app_args = parse_app_args(cfg=cfg, ui_args=kwargs)
    backend_key = app_args[const.MAINARG_PROCESSING_BACKEND]
    dry_run = app_args.get(const.MAINARG_DRY_RUN, False)
    save_downloaded = app_args.get(const.MAINARG_SAVE_DOWNLOADED, False)
    save_normalized = app_args.get(const.MAINARG_SAVE_NORMALIZED, False)
    batch = 'not started'

    fetcher = get_fetcher(app_args=app_args)

    if not dry_run:
        backend = get_backend(backend_key=backend_key)

        while batch:
            batch = next(fetcher, None)

            for randaddr, randfile in batch.values():
                extracted = backend.extract_item(
                    backend_key=backend,
                    addr=randaddr,
                    fname=randfile
                )

                if save_downloaded:
                    extracted_savepath = backend.save_extracted(extracted=extracted)
                    print(f"Extracted data saved to {extracted_savepath} successfully.")
                    continue

                normalized = backend.normalize_item(
                    extracted=extracted,
                    backend_key=backend_key
                )

                if save_normalized:
                    normalized_savepath = backend.save_normalized(normalized=normalized)
                    print(f"Normalized data saved to {normalized_savepath} successfully.")
                    continue

    else: print("Dry Run!")
    return True

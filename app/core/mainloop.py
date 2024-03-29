import os
import typing

import app.constants as const
from app.core.fetch.fetching import fetch_all
from app.processing_backends import get_backend
from app.utils.logs import logger


def parse_app_args(config: object = None, ui_args: dict = None) -> dict:
    """Cleans, infers, defaults, and translates the app arguments for main() from raw UI args.
    A UI -> core adapter, in other words.

    :param config: Configuration instance
    :param ui_args: Raw parameters from the UI, as a dict.
    """
    _app_args = ui_args or dict()
    cfg = config or dict()

    # Any required keywords (ANDed together!) in the free text search over all categories
    free_text_query = (
        '+AND+'.join((
            _app_args.get(const.MAINARG_QUERY)
            or []
        ))
        # deliberately OUTSIDE of the join() to allow more customization
        or cfg.get("query", {}).get("text", None)
    )

    # Species (e.g. Homo Sapiens or S. Cerevisiae)
    species = (
            _app_args.get(const.MAINARG_ORGANISM)
            or cfg.get("query", {}).get("organism", None)
    )
    species_query = '{species}[Organism]'.format(species=species) if species else None

    # Entry type (DataSet, Series, Samples, Platforms...)
    entrytype = (
            None  # TODO: add CLI arg for it
            or cfg.get("query", {}).get("entrytype", None)
    )
    entrytype_query = '{entrytype}[EntryType]'.format(entrytype=entrytype) if entrytype else None

    # Supplementary file format (e.g. CEL, GPR, WIG... - to filter down to only what we can parse)
    fileformat = (
            None  # TODO: add CLI arg for it
            or cfg.get("query", {}).get("fileformat", None)
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
        or (cfg.get("batch_size") if cfg else None)
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
        or (cfg.get("backend")if cfg else None)
        or const.BACKEND_LOCAL
    )
    results[const.MAINARG_PRECALCULATED_SOURCES] = dict(cfg.get("accession_numbers", {})) if cfg else None
    results[const.MAINARG_DRY_RUN] = bool(cfg.get("dry_run")) if cfg else False
    results[const.MAINARG_SAVE_DOWNLOADED] = _app_args.get(const.MAINARG_SAVE_DOWNLOADED, False)
    results[const.MAINARG_SAVE_NORMALIZED] = _app_args.get(const.MAINARG_SAVE_NORMALIZED, False)

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


def process_item(
    addr,
    fname,
    backend,
    save_downloaded=False,
    save_normalized=False
):
    # Download:
    extracted = backend.extract_item(
        backend_key=backend,
        addr=addr,
        fname=fname
    )

    if save_downloaded:
        in_savedir = os.path.join(
            const.BASE_DIR,
            "outputs",
            "extracted",
        )
        os.makedirs(in_savedir, exist_ok=True)
        in_savepath = os.path.join(in_savedir, f"{fname}.json")

        extracted_savepath = backend.save_extracted(
            extracted=extracted,
            file=in_savepath
        )

        logger.info(f"Extracted data saved to {extracted_savepath} successfully.")
        return extracted_savepath

    # Transform:
    normalized = backend.normalize_item(
        extracted=extracted,
    )

    if save_normalized:
        in_savedir = os.path.join(
            const.BASE_DIR,
            "outputs",
            "normalized",
        )
        os.makedirs(in_savedir, exist_ok=True)
        in_savepath = os.path.join(in_savedir, f"{fname}.json")

        normalized_savepath = backend.save_normalized(
            normalized=normalized,
            file=in_savepath
        )

        logger.info(f"Normalized data saved to {normalized_savepath} successfully.")
        return normalized_savepath

    return normalized


def coreloop(cfg=None, **kwargs):
    """Core loop of the pipeline.

    Purely programmatic - human-friendly UIs can slot into it by creating adapters
    that inject kwargs into this function.

    All supported parameter keys are constants with the `MAINARG_` prefix.
    """
    app_args = parse_app_args(config=cfg, ui_args=kwargs)
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
                output = process_item(
                    addr=randaddr,
                    fname=randfile,
                    backend=backend,
                    save_downloaded=save_downloaded,
                    save_normalized=save_normalized
                )
                yield output

    else: logger.warn("Dry Run!")
    return True


def main(cfg=None, **kwargs):
    """Root of the pipeline.

    Purely programmatic - human-friendly UIs can slot into it by creating adapters
    that inject kwargs into this function.

    All supported parameter keys are constants with the `MAINARG_` prefix.
    """
    loop = coreloop(cfg=cfg, **kwargs)
    for data in loop:
        pass

import time

import constants as const
from parsers import parse_format, infer_format
from core.search import NcbiDbs
from core.search.esearch import get_query_env
from core.search.esummary import build_search_url, get_search_results, parse_search_response
from utils.ftp import extract_ftp_links, build_soft_ftp_url, fetch_ftp


def fetch_from_pos(
    position: int,
    term: str,
    db=NcbiDbs.GDS.value,
    batch_size=const.DEFAULT_SEARCH_INCREMENT,
    query_env=None
) -> dict:

    _position = max(position, 1)
    _maxsize = max(batch_size, 1)

    webenv, qkey = query_env if query_env else get_query_env(term=term, db=db)

    search_url = build_search_url(webenv=webenv, query_key=qkey, retstart=_position, retmax=_maxsize)
    raw_search_results = get_search_results(search_url)
    search_results = parse_search_response(raw_search_results)
    
    raw_links = extract_ftp_links(search_results)
    download_links = {
        data_id: build_soft_ftp_url(raw_link)
        for (data_id, raw_link)
        in raw_links.items()
    }

    return download_links


def fetch_all(term: str, db=NcbiDbs.GDS.value, batch_size=None):
    result = None
    remaining_data = True
    query_env = get_query_env(term=term, db=db)

    curr_batch_size = const.DEFAULT_SEARCH_INCREMENT if batch_size is None else max(batch_size, 1)
    new_batch_size = None

    curr_pos = 1

    while remaining_data:
        curr_batch_size = curr_batch_size if new_batch_size is None else max(new_batch_size, 1)
        result = fetch_from_pos(
            curr_pos,
            term=term,
            db=db,
            batch_size=curr_batch_size,
            query_env=query_env
        )
        if not result: remaining_data = False
        curr_pos += batch_size
        new_batch_size = yield result

    return result


def process_item(addr, fname):
    raw_result, ftp_error = fetch_ftp(addr, fname)
    if ftp_error:
        print(ftp_error)
    else:
        print(raw_result)
    parsed_result = parse_format(data=raw_result, dataformat=infer_format(fname))
    return parsed_result


def main(term: str, db=NcbiDbs.GDS.value, increment=None):
    _batch_size = const.DEFAULT_SEARCH_INCREMENT if increment is None else max(increment, 1)

    fetcher = fetch_all(term=term, db=db, batch_size=_batch_size)
    batch = 'not started'

    while batch:
        batch = next(fetcher, None)
        randaddr, randfile = batch.popitem()[-1]
        result = process_item(addr=randaddr, fname=randfile)
        time.sleep(1)

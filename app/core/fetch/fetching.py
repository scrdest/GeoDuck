import constants as const
from core.search import NcbiDbs, esummary, esearch
from utils.ftp import extract_ftp_links, build_soft_ftp_url


def fetch_from_pos(
    position: int,
    term: str,
    db=NcbiDbs.GDS.value,
    batch_size=const.DEFAULT_SEARCH_INCREMENT,
    query_env=None
) -> dict:

    _position = max(position, 1)
    _maxsize = max(batch_size, 1)

    webenv, qkey = query_env if query_env else esearch.get_query_env(term=term, db=db)

    search_url = esummary.build_search_url(webenv=webenv, query_key=qkey, retstart=_position, retmax=_maxsize)
    raw_search_results = esummary.get_search_results(search_url)
    search_results = esummary.parse_search_response(raw_search_results)

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
    query_env = esearch.get_query_env(term=term, db=db)

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

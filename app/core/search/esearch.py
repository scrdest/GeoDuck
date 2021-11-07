import json

import requests

import app.constants as const
from app.utils.decorators import with_print


@with_print(pretty=False, disabled=True)
def build_query_url(
    term: str = None,
    database: str = None,
    retstart: int = 1,
    retmax: int = const.DEFAULT_QUERY_INCREMENT,
    use_history: bool = True
    ) -> str:
    """Builds a NCBI search query URL based on the provided options:

    :param term: Search term, e.g. 'cancer' or 'yeast[orgn]', as string.
    :param database: Database to query. Should be a valid Entrez database name, as string.
    :param retstart: Starting page offset, used for paging the results.
    :param retmax: Maximum records per page. Higher means more memory, but more records retrieved / second.
    :param use_history: Controls query caching on the tool side.
                        Included for integration completeness' sake, but you almost certainly should
                        let it stay True if you want to reuse the search results downstream.

    :returns: NCBI search query URL, as a string
    """

    db_param = f"db={database}" if database else None
    term_param = f"term={term}" if term else None
    retstart_param = f"retstart={max(1, retstart)}" if retstart else 1
    retmax_param = f"retmax={retmax}" if retmax else None
    fmt_param = f"retmode=json"
    usehistory_param = (
        None if use_history is None
        else "usehistory={use_history}".format(
            use_history=('y' if use_history else 'n')
        )
    )

    sanitized_params = "&".join(
        (p for p in (
            db_param,
            term_param,
            usehistory_param,
            fmt_param,
            retstart_param,
            retmax_param
        ) if p)
    )

    req_url = const.NCBI_QUERY_URL_TEMPLATE.format(
        query_base=const.BASE_NCBI_QUERY_URL,
        params=sanitized_params
    )

    return req_url


@with_print(pretty=True, disabled=True)
def run_query(query_url: str) -> str:
    """Runs a HTTP request against a specified URL.

    :param query_url: URL to query.
    :returns: remote response, as raw string text.
    """
    # Thin wrapper, mostly for decoratability purposes,
    # since Requests is just way too good like that.
    result = requests.get(query_url).text
    return result


@with_print(pretty=True, disabled=True)
def parse_query_response(qry_response: str) -> dict:
    """Parses the raw API response as returned by run_query()
    into an internal data format for processing.

    :param qry_response: Raw query response, as string.
    :returns: A dictionary of parsed data.
    """
    json_doc = json.loads(qry_response)
    search_result = json_doc.get(const.QUERY_RESULT_FIELD, {})
    return search_result


def get_query_env(term: str, db=const.DEFAULT_DB_VALUE) -> tuple:
    """Builds and executes an ESearch query, retrieving the Webenv and Query Key parameters from the API
    for use by the paginated queries downstream.

    :param term: Search term, e.g. 'cancer' or 'yeast[orgn]', as string.
    :param db: Optional. Database to query. Should be a valid Entrez database name, as string.
    """
    query_url = build_query_url(term=term, database=db, retstart=1, retmax=1)
    query_response = run_query(query_url=query_url)

    response_dict = parse_query_response(query_response)
    webenv = response_dict.get(const.QUERY_WEBENV_FIELD)
    qkey = response_dict.get(const.QUERY_QRYKEY_FIELD)
    return webenv, qkey

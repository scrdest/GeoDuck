import json

import requests

import constants as const
from app.utils.decorators import with_print


@with_print(
    pretty=False,
    disabled=True
)
def build_query_url(
    term=None,
    database=None,
    retstart=1,
    retmax=const.DEFAULT_QUERY_INCREMENT,
    use_history=True
    ) -> str:

    db_param = f"db={database}" if database else None
    term_param = f"term={term}" if term else None
    retstart_param = f"retstart={retstart}" if retstart else 1
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
        query_base = const.BASE_NCBI_QUERY_URL,
        params = sanitized_params
    )

    return req_url


@with_print(pretty=True, disabled=True)
def run_query(query_url):
    result = requests.get(query_url).text
    return result


@with_print(pretty=True, disabled=True)
def parse_query_response(qry_response):
    json_doc = json.loads(qry_response)
    search_result = json_doc.get(const.QUERY_RESULT_FIELD, {})
    return search_result


def get_query_env(term: str, db=const.DEFAULT_DB_VALUE) -> tuple:
    query_url = build_query_url(term=term, database=db, retstart=1, retmax=1)
    query_response = run_query(query_url=query_url)

    response_dict = parse_query_response(query_response)
    webenv = response_dict.get(const.QUERY_WEBENV_FIELD)
    qkey = response_dict.get(const.QUERY_QRYKEY_FIELD)
    return (webenv, qkey)

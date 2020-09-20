import sys
import json

import requests

import constants as const
from app.utils.decorators import with_print


@with_print(
    pretty=True,
    disabled=True
)
def build_search_url(
    webenv,
    query_key='',
    database=None,
    retstart=1,
    retmax=const.DEFAULT_SEARCH_INCREMENT,
    ) -> str:

    db_param = f"db={database}" if database else None
    version_param = f"version=2.0"
    webenv_param = f"WebEnv={webenv}"
    retstart_param = f"retstart={retstart}" if retstart else 1
    retmax_param = f"retmax={retmax}" if retmax else None
    qkey_param = f"query_key={query_key}" if query_key else None
    fmt_param = f"retmode=json"

    sanitized_params = "&".join(
        (p for p in (
            db_param,
            version_param,
            qkey_param,
            webenv_param,
            fmt_param,
            retstart_param,
            retmax_param
        ) if p)
    )

    req_url = const.NCBI_SUMMARY_URL_TEMPLATE.format(
        search_base=const.BASE_NCBI_SUMMARY_URL,
        params=sanitized_params
    )

    return req_url


@with_print(pretty=True, disabled=True)
def get_search_results(search_url):
    result = requests.get(search_url).text
    return result


@with_print(
    pretty=True,
    disabled=True
)
def parse_search_response(qry_response):
    uid_data = {}
    if qry_response:
        try:
            doc = json.loads(qry_response)
            search_result = doc.get(const.SEARCH_RESULT_FIELD, {})
            uids = set(search_result.get(const.SEARCH_UIDS_FIELD) or [])
            uid_data = {
                uID: data
                for (uID, data) in search_result.items()
                if uID in uids
            }
        except Exception as E:
            sys.excepthook(*sys.exc_info())
    return uid_data

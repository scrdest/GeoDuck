import enum
import json
import time
from ftplib import FTP, error_perm

import requests

import constants as const
from decorators import with_print
from utils.ftp import extract_ftp_links, build_soft_ftp_url, ftp_read


class NcbiDbs(enum.Enum):
    GDS = "gds"


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
def run_query(query_url):
    result = requests.get(query_url).text
    return result
    
    
@with_print(pretty=True, disabled=True)
def parse_query_response(qry_response):
    xml_doc = json.loads(qry_response)
    search_result = xml_doc.get(const.QUERY_RESULT_FIELD, {})
    return search_result
    
    
@with_print(pretty=True, disabled=True)
def get_search_results(search_url):
    result = requests.get(search_url).text
    return result
    
    
@with_print(
    pretty=True, 
    disabled=True
)
def parse_search_response(qry_response):
    doc = json.loads(qry_response)
    search_result = doc.get(const.SEARCH_RESULT_FIELD, {})
    uids = set(search_result.get(const.SEARCH_UIDS_FIELD) or [])
    uid_data = {
        uID: data 
        for (uID, data) in search_result.items() 
        if uID in uids
    }
    return uid_data


def get_query_env(term: str, db=NcbiDbs.GDS.value) -> tuple:
    query_url = build_query_url(term=term, database=db, retstart=1, retmax=1)
    query_response = run_query(query_url=query_url)

    response_dict = parse_query_response(query_response)
    webenv = response_dict.get(const.QUERY_WEBENV_FIELD)
    qkey = response_dict.get(const.QUERY_QRYKEY_FIELD)
    return (webenv, qkey)


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


def main(term: str, db=NcbiDbs.GDS.value, increment=None):
    _batch_size = const.DEFAULT_SEARCH_INCREMENT if increment is None else max(increment, 1)
    with FTP('ftp.ncbi.nlm.nih.gov') as ftp_client:
        ftp_client.login()
        fetcher = fetch_all(term=term, db=db, batch_size=_batch_size)
        batch = 'not started'
        while batch:
            batch = next(fetcher, None)
            randaddr, randfile = batch.popitem()[-1]
            try:
                ftp_client.cwd('/')
                ftp_client.cwd(randaddr)
                ftp_client.retrbinary(f'RETR {randfile}', ftp_read)
            except error_perm as E:
                print(E)
            time.sleep(1)

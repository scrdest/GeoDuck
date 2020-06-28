import os
import sys
import enum

import requests
import json

import constants as const
from decorators import with_print


class NcbiDbs(enum.Enum):
    GDS = "gds"


@with_print(
    pretty=False, 
    disabled=True
)
def build_query_url(
    term=None, 
    database=None,
    retmax=20, 
    use_history=True
    ) -> str:
    
    db_param = f"db={database}" if database else None
    term_param = f"term={term}" if term else None
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
            retmax_param,
            usehistory_param,
            fmt_param
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
    retmax=100, 
    ) -> str:
    
    db_param = f"db={database}" if database else None
    version_param = f"version=2.0"
    webenv_param = f"WebEnv={webenv}"
    retmax_param = f"retmax={retmax}" if retmax else None
    qkey_param = f"query_key={query_key}" if query_key else None
    fmt_param = f"retmode=json"
    
    sanitized_params = "&".join(
        (p for p in (
            db_param, 
            qkey_param, 
            version_param, 
            webenv_param,
            retmax_param,
            fmt_param
        ) if p)
    )

    req_url = const.NCBI_SUMMARY_URL_TEMPLATE.format(
        search_base = const.BASE_NCBI_SUMMARY_URL,
        params = sanitized_params
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
    
    
@with_print(
    pretty=True, 
    disabled=False
)
def extract_ftp_links(search_result):
    links = {
        id: data[const.FTP_LINK_FIELD] 
        for (id, data) in search_result.items()
        if data.get(const.FTP_LINK_FIELD)
    }
    return links
    

def build_ftp_url(ftp_link):
    raise NotImplementedError

    
def to_jsonl(iterable, base_filename=None):
    import json
    _base_filename = base_filename or os.path.join(const.OUTPUT_DIR, 'result')
    json_stream = map(json.dumps, iterable)
    
    files = {}
    try:
        for (idx, item) in enumerate(iterable):
            print(idx)
            filename = '{base}_{num}.json'.format(
                base=_base_filename,
                num=(idx // 1000)
            )
            fd = files.setdefault(filename, open(filename, 'w'))
            fd.write(json.dumps(item))
            fd.write('\n')
    finally:
        for fd in files.values(): 
            fd.close()
    return True


def main(term: str, db=NcbiDbs.GDS.value):
    result = None
    query_url = build_query_url(term=term, database=db)
    query_response = run_query(query_url=query_url)
    
    response_dict = parse_query_response(query_response)
    webenv = response_dict.get(const.QUERY_WEBENV_FIELD)
    qkey = response_dict.get(const.QUERY_QRYKEY_FIELD)
    
    search_url = build_search_url(webenv=webenv, query_key=qkey)
    raw_search_results = get_search_results(search_url)
    search_results = parse_search_response(raw_search_results)
    
    links = extract_ftp_links(search_results)
    
    return result
 

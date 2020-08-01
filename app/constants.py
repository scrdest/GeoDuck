import os

DEFAULT_QUERY_INCREMENT = 1
DEFAULT_SEARCH_INCREMENT = 10

DEFAULT_INTERFACE_REGISTRY_KEY = 'interface'
DEFAULT_PARSER_REGISTRY_KEY = 'parsers'

ROOT_DIR = os.path.dirname(__file__)
BASE_DIR = os.path.dirname(ROOT_DIR)
OUTPUT_DIR = os.path.join(ROOT_DIR, 'results')

ARG_INTERFACE = 'AppInterface'
ARG_DATABASE = 'Database'
ARG_QUERY = 'Query'
ARG_TERM = 'Term'
ARG_ORGANISM = 'Organism'
ARG_FORMAT = 'Format'

MAINARG_DATABASE = 'db'
MAINARG_INCREMENT = 'increment'
MAINARG_BATCH_SIZE = 'batch_size'

INTERFACE_CLI = 'cli'

BASE_NCBI_QUERY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
NCBI_QUERY_URL_TEMPLATE = "{query_base}?{params}"

BASE_NCBI_SUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
NCBI_SUMMARY_URL_TEMPLATE = "{search_base}?{params}"

FTP_LINK_FIELD = 'ftplink'

QUERY_RESULT_FIELD = 'esearchresult'
QUERY_WEBENV_FIELD = 'webenv'
QUERY_QRYKEY_FIELD = 'querykey'

SEARCH_RESULT_FIELD = 'result'
SEARCH_UIDS_FIELD = 'uids'

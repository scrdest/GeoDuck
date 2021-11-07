import os
import sys
import enum

APP_NAME = 'GeoDuck'

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_DIR = os.path.join(ROOT_DIR, 'config')
OUTPUT_DIR = os.path.join(ROOT_DIR, 'results')
BASE_DIR = os.path.dirname(ROOT_DIR)

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

os.makedirs(CONFIG_DIR, exist_ok=True)


class NcbiDbs(enum.Enum):
    DataSets = "gds"
    Profiles = "geoprofiles"


DEFAULT_DB_VALUE = NcbiDbs.DataSets.value

ENV_CONFIGNAME_KEY = 'config_name'
DEFAULT_CONFIG_FILENAME = os.path.join('config', 'config.yaml')


DEFAULT_QUERY_INCREMENT = 1
DEFAULT_SEARCH_INCREMENT = 1

DEFAULT_INTERFACE_REGISTRY_KEY = 'interface'
DEFAULT_BACKEND_REGISTRY_KEY = 'backends'
DEFAULT_PARSER_REGISTRY_KEY = 'parsers'

DEFAULT_EXTRACTED_SAVE_FILENAME = "extracted.txt"
DEFAULT_NORMALIZED_SAVE_FILENAME = "normalized.txt"

ARG_INTERFACE = 'AppInterface'

MAINARG_QUERY = 'Query'
MAINARG_ORGANISM = 'Organism'
MAINARG_PRECALCULATED_SOURCES = 'precalculated_sources'
MAINARG_DATABASE = 'db'
MAINARG_INCREMENT = 'increment'
MAINARG_BATCH_SIZE = 'batch_size'
MAINARG_PROCESSING_BACKEND = 'processing_backend'
MAINARG_DRY_RUN = 'dry_run'
MAINARG_SAVE_DOWNLOADED = 'save_downloaded'
MAINARG_SAVE_NORMALIZED = 'save_normalized'

INTERFACE_NONE = 'none'
INTERFACE_CLI = 'cli'

BACKEND_LOCAL = 'local'
BACKEND_SPARK = 'pyspark'

PARSER_GENERIC = 'generic'

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

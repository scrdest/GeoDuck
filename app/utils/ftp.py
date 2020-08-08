import ftplib
import tempfile

from smart_open import open

import constants as const
from decorators import with_print

from utils.spark import SPARK_SUPPORT

ftp_client_builder = ftplib.FTP


@with_print(
    pretty=True,
    disabled=True
)
def extract_ftp_links(search_result):
    links = {
        id: data[const.FTP_LINK_FIELD]
        for (id, data) in search_result.items()
        if data.get(const.FTP_LINK_FIELD)
    }
    return links


@with_print(
    pretty=True,
    disabled=False
)
def build_soft_ftp_url(raw_ftp_link: str) -> tuple:
    entry_name = raw_ftp_link.rstrip('/').split('/')[-1]
    protocol_adjusted_link = raw_ftp_link.replace('ftp://ftp.ncbi.nlm.nih.gov/', '', 1)
    download_ftp_path = ''.join((protocol_adjusted_link, 'suppl/'))
    download_ftp_filename = entry_name# + '.soft.gz' #+ '_family.soft.gz'
    return download_ftp_path, download_ftp_filename


class FTPReader:
    def __init__(self, fname=None):
        self.fname = fname
        self.storage = tempfile.NamedTemporaryFile()

    def ftp_read(self, data):
        self.storage.write(data)

    def __call__(self, *args, **kwargs):
        self.ftp_read(*args, **kwargs)

    def parse_to_raw_result(self, datastream=None, filename=None):
        from gzip import GzipFile
        result = None
        _data = datastream or self.storage
        fname = filename or self.fname
        if fname:
            print(fname)

        try:
            _data.seek(0)
            bstream = GzipFile(fileobj=_data)

            with open(bstream, 'r') as zipdata:
                result = zipdata.read()

        except Exception as E:
            print(E)

        finally:
            self.storage.close()

        return result


def rebuild_client():
    client = ftp_client_builder('ftp.ncbi.nlm.nih.gov')
    client.login()
    return client


def ftp_switchcwd(address, ftp_client):
    ftp_client.cwd('/')
    for subdir in address.split('/'):
        ftp_client.cwd(subdir)
    return True


def ftp_listdir(address, client=None):
    ftp_client = client or rebuild_client()
    err = None
    results = None

    try:
        ftp_switchcwd(address, ftp_client=ftp_client)
        results = tuple(ftp_client.mlsd())

    except ftplib.error_perm as E:
        err = E

    finally:
        if not client:
            # we created one, so we're closing it
            ftp_client.close()

    return err if err else results


def fetch_ftp(address, filename, client=None):
    result, err = None, None
    ftp_client = client or rebuild_client()

    try:
        file_list = ftp_listdir(address=address, client=ftp_client)

        for (name, metadata) in file_list:
            if filename in name:
                target = name

                reader = FTPReader(address + target)

                try:
                    ftp_client.retrbinary(f'RETR {target}', reader)

                except ftplib.error_temp:
                    ftp_client = rebuild_client()
                    ftp_client.retrbinary(f'RETR {target}', reader)

                result = reader.parse_to_raw_result()

    except ftplib.error_perm as E:
        err = E

    finally:
        if not client:
            # we created one, so we're closing it
            ftp_client.close()

    return result, err

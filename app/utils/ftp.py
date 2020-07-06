from smart_open import open

import constants as const
from decorators import with_print


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
    download_ftp_path = ''.join((protocol_adjusted_link, 'soft/'))
    download_ftp_filename = entry_name + '_family.soft.gz'
    return download_ftp_path, download_ftp_filename


class FTPReader:
    def __init__(self, fname=None):
        from io import BytesIO
        self.fname = fname
        self.storage = BytesIO()

    def ftp_read(self, data):
        self.storage.write(data)

    def __call__(self, *args, **kwargs):
        self.ftp_read(*args, **kwargs)

    def parse_result(self, datastream=None, filename=None):
        from gzip import GzipFile
        fname = filename or self.fname
        _data = datastream or self.storage
        if fname: print(fname)
        try:
            _data.seek(0)
            bstream = GzipFile(fileobj=_data)
            with open(bstream, 'r') as zipdata:
                print(zipdata.read())
        except Exception as E:
            print(E)
        self.storage.close()

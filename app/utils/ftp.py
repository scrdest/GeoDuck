import ftplib
import tempfile
import typing

from smart_open import open

import app.constants as const
from app.utils.decorators import with_print, with_logging
from app.utils.logs import logger

ftp_client_builder = ftplib.FTP
FtpClientType = ftplib.FTP


@with_logging(pretty=True, disabled=False)
@with_print(pretty=True, disabled=True)
def extract_ftp_links(search_result) -> typing.Dict[typing.Hashable, dict]:
    links = {
        id: data[const.FTP_LINK_FIELD]
        for (id, data) in search_result.items()
        if data.get(const.FTP_LINK_FIELD)
    }
    return links


@with_logging(pretty=True, disabled=False)
@with_print(pretty=True, disabled=True)
def build_matrix_ftp_url(raw_ftp_link: str) -> typing.Tuple[str, str]:
    entry_name = raw_ftp_link.rstrip('/').split('/')[-1]
    protocol_adjusted_link = raw_ftp_link.replace('ftp://ftp.ncbi.nlm.nih.gov/', '', 1)
    download_ftp_path = ''.join((protocol_adjusted_link, 'matrix/'))
    download_ftp_filename = entry_name
    return download_ftp_path, download_ftp_filename


class FTPReader:
    def __init__(self, fname: typing.Optional[str] = None):
        self.fname = fname
        self.storage = tempfile.NamedTemporaryFile()

    def ftp_read(self, data: typing.AnyStr) -> typing.NoReturn:
        self.storage.write(data)

    def __call__(self, *args, **kwargs) -> typing.NoReturn:
        self.ftp_read(*args, **kwargs)

    def parse_to_raw_result(
        self,
        datastream: typing.Optional[typing.IO] = None,
        filename: typing.Optional[str] = None
    ) -> typing.AnyStr:

        from gzip import GzipFile
        result = None
        _data = datastream or self.storage
        fname = filename or self.fname
        if fname:
            logger.info(f"Processing filename: {fname}")

        try:
            _data.seek(0)
            bstream = GzipFile(fileobj=_data)

            with open(bstream, 'r') as zipdata:
                result = zipdata.read()

        except Exception as E:
            logger.exception(E)

        finally:
            self.storage.close()

        return result


def rebuild_client() -> FtpClientType:
    client = ftp_client_builder('ftp.ncbi.nlm.nih.gov')
    client.login()
    return client


def ftp_switchcwd(address: str, ftp_client: FtpClientType) -> bool:
    ftp_client.cwd('/')
    for subdir in address.split('/'):
        ftp_client.cwd(subdir)
    return True


def ftp_listdir(address, client: typing.Optional[FtpClientType] = None) -> typing.List[typing.Tuple[str, typing.Union[dict, typing.Any]]]:
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

    if err:
        results = [('', err)]

    return results


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
                    # The callback accumulates blocks of data in an IO object here
                    ftp_client.retrbinary(cmd=f'RETR {target}', callback=reader.ftp_read)

                except ftplib.error_temp:
                    ftp_client = rebuild_client()
                    ftp_client.retrbinary(cmd=f'RETR {target}', callback=reader.ftp_read)

                # Since the FTP reads are (sadly) stateful, the
                # data to parse is smuggled in the state here:
                result = reader.parse_to_raw_result()
                if not result:
                    raise ValueError(f"Failed to parse the results for {target}")

    except ftplib.error_perm as E:
        err = E

    except ValueError as E:
        err = E

    finally:
        if not client:
            # we created one, so we're closing it
            ftp_client.close()

    return result, err

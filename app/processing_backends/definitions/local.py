import constants as const
from abcs import AbstractProcessingBackend
from app.parsers import parse_format, infer_format

from app.utils.ftp import fetch_ftp
from app.utils.registry import registry_entry


@registry_entry(as_key='local', registry_key=const.DEFAULT_BACKEND_REGISTRY_KEY)
class LocalProcessingBackend(AbstractProcessingBackend):
    """A pure Python implementation of the processing pipeline.
    Not very efficient; intended as mostly a fallback solution.
    """
    @classmethod
    def process_item(cls, addr: str, fname: str, *args, **kwargs):
        """The main processing pipeline for a single source URL.

        :param addr: Path to the source FTP directory
        :param fname: Filename in the source FTP directory
        """
        raw_result, ftp_error = fetch_ftp(addr, fname)
        if ftp_error:
            print(ftp_error)
        else:
            print(raw_result)
        parsed_result = parse_format(data=raw_result, dataformat=infer_format(fname))
        return parsed_result

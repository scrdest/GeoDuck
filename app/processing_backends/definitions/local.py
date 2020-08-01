import constants as const
from abcs import AbstractProcessingBackend
from parsers import parse_format, infer_format

from utils.ftp import fetch_ftp
from utils.registry import registry_entry


@registry_entry(as_key='local', registry_key=const.DEFAULT_BACKEND_REGISTRY_KEY)
class LocalProcessingBackend(AbstractProcessingBackend):
    @classmethod
    def process_item(cls, addr, fname, *args, **kwargs):
        raw_result, ftp_error = fetch_ftp(addr, fname)
        if ftp_error:
            print(ftp_error)
        else:
            print(raw_result)
        parsed_result = parse_format(data=raw_result, dataformat=infer_format(fname))
        return parsed_result

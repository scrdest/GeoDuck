import os

from app.abcs import AbstractProcessingBackend
from app.parsers import parse_format, infer_format

from app.utils.ftp import fetch_ftp

import typing

import app.constants as const
from app.utils.functional import tumap, tufilter
from app.utils.registry import registry_entry


def parse_exclamation_as_key(
    data: typing.Iterable[str]
) -> typing.Tuple[typing.Optional[str], typing.Tuple[str]]:

    key, vals = None, tuple()
    data_range = data

    data_iter = iter(data)
    first_itm = next(data_iter, None)

    if first_itm:
        if first_itm.startswith("!"):
            key = first_itm[1:]
            data_range = data[1:]

        elif first_itm.upper() == '"ID_REF"':
            key = "ID_REF"
            data_range = data[1:]

    vals = tuple((x.strip('"') for x in data_range))

    return key, vals



@registry_entry(as_key='local', registry_key=const.DEFAULT_BACKEND_REGISTRY_KEY)
class LocalProcessingBackend(AbstractProcessingBackend):
    """A pure Python implementation of the processing pipeline.
    Not very efficient; intended as mostly a fallback solution.
    """

    @classmethod
    def extract_item(cls, addr: str, fname: str, *args, **kwargs):
        """The main processing pipeline for a single source URL.

        :param addr: Path to the source FTP directory
        :param fname: Filename in the source FTP directory
        """
        raw_result, ftp_error = fetch_ftp(addr, fname)
        if ftp_error:
            print(ftp_error)
        else:
            print(raw_result)
        parsed_result = parse_format(data=raw_result, dataformat=infer_format(fname)) if raw_result else raw_result
        return parsed_result


    @classmethod
    def save_extracted(cls, extracted: str, file: typing.Optional[os.PathLike] = None, *args, **kwargs) -> os.PathLike:
        """Write out the results of extract_item to a file."""
        _filepath = file or const.DEFAULT_EXTRACTED_SAVE_FILENAME
        saved = False

        with open(_filepath, "w") as dumpfile:
            dumpfile.write(extracted)

        saved = True
        return _filepath if saved else None


    @classmethod
    def normalize_item(cls, extracted, *args, **kwargs) -> typing.Dict[str, typing.Tuple[str]]:
        data_items = (
            extracted if isinstance(extracted, typing.Iterable)
            and not isinstance(extracted, typing.Sequence)
            else [extracted]
        )
        data_lines = tumap(lambda itm: itm.split("\n"), data_items)
        nonempty_lines = tufilter(None, data_lines)
        columns = tumap(lambda l: tumap(lambda x: x.split('\t'), l), nonempty_lines)
        key_valued = tumap(lambda c: tumap(parse_exclamation_as_key, c), columns)

        parsed = {}
        duplicate_keys = {}

        for series in key_valued:
            for (key, vals) in series:
                if not vals or not tufilter(None, vals, reify=True):
                    continue

                used_key = key

                if key in parsed:
                    duplicate_count = duplicate_keys.get(key, 1) + 1
                    duplicate_keys[key] = duplicate_count
                    used_key = f"{key}-{duplicate_count}"

                parsed[used_key] = vals

        return parsed


    @classmethod
    def save_normalized(cls, normalized: str, file: typing.Optional[os.PathLike] = None, *args, **kwargs) -> os.PathLike:
        """Write out the results of normalize_item to a file."""
        _filepath = file or const.DEFAULT_NORMALIZED_SAVE_FILENAME
        saved = False

        with open(_filepath, "w") as dumpfile:
            dumpfile.write(normalized)

        saved = True
        return _filepath if saved else None

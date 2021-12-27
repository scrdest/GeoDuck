import copy
import os

from app.abcs import AbstractProcessingBackend
from app.parsers import parse_format, infer_format

from app.utils.ftp import fetch_ftp

import re
import typing

import app.constants as const
from app.utils.functional import tumap, tufilter
from app.utils.registry import registry_entry
from app.utils.logs import logger
from app.utils.ftp import build_data_ftp_url


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
    data_storage_class = dict

    @classmethod
    def extract_item(cls, addr: str, fname: str, *args, **kwargs):
        """The main processing pipeline for a single source URL.

        :param addr: Path to the source FTP directory
        :param fname: Filename in the source FTP directory
        """
        raw_result, ftp_error = fetch_ftp(addr, fname)
        if ftp_error:
            logger.error(ftp_error)
        parsed_result = parse_format(data=raw_result, dataformat=infer_format(fname)) if raw_result else raw_result
        return parsed_result


    @classmethod
    def save_extracted(cls, extracted: str, file: typing.Optional[os.PathLike] = None, *args, **kwargs) -> os.PathLike:
        """Write out the results of extract_item to a file."""
        _filepath = file or const.DEFAULT_EXTRACTED_SAVE_FILENAME
        import json
        saved = False

        with open(_filepath, "w") as dumpfile:
            json.dump(extracted, dumpfile, indent=4)

        saved = True
        return _filepath if saved else None


    @classmethod
    def normalize_item(
        cls,
        extracted,
        pad_lengths=True,
        *args,
        **kwargs
    ) -> typing.Dict[str, typing.Tuple[str]]:

        # Input standardization:
        data_items = (
            extracted if isinstance(extracted, typing.Iterable)
            and not isinstance(extracted, typing.Sequence)
            else [extracted]
        )

        # Preprocessing pipeline (lazy, unless passed 'reify=True'):
        safe_data_items = tufilter(None, data_items)
        data_lines = tumap(lambda itm: itm.split("\n"), safe_data_items)
        nonempty_lines = tufilter(None, data_lines)
        columns = tumap(lambda l: tumap(lambda x: x.split('\t'), l), nonempty_lines)
        key_valued = tumap(lambda c: tumap(parse_exclamation_as_key, c), columns)

        # Setting up the bookkeeping:
        parsed = {}
        duplicate_keys = {}
        column_lengths = {}
        max_row_length = 0

        for series in key_valued:
            for (key, vals) in series:
                if not vals or not tufilter(None, vals, reify=True):
                    continue

                # Reindex duplicate key names:
                used_key = key

                if key in parsed:
                    duplicate_count = duplicate_keys.get(key, 1) + 1
                    duplicate_keys[key] = duplicate_count
                    used_key = f"{key}-{duplicate_count}"

                parsed[used_key] = vals

                # Track tuple size for padding out short rows later:
                row_length = len(vals)
                if row_length > max_row_length:
                    max_row_length = row_length

                column_lengths.setdefault(row_length, []).append(used_key)

        if pad_lengths:
            # NOTE: there's a case to be made for this being a separate function;
            #       keeping it in here for now to avoid passing dicts around weirdly.
            # Pad out rows to uniform length so that we can make columns independent:
            for curr_row_length, adjusted_keys in column_lengths.items():
                if curr_row_length == max_row_length:
                    continue

                padding_size = (max_row_length - curr_row_length)

                for short_key in adjusted_keys:
                    curr_val = parsed[short_key]

                    # Rough heuristic - most of the time, the 'paddable' rows
                    # will just have one value to explode to all columns:
                    first_val_item = next(iter(curr_val))

                    # Append as many copies as needed to match the max rowsize:
                    parsed[short_key] = curr_val + tuple((
                        copy.deepcopy(first_val_item)
                        for _ in range(padding_size)
                    ))

        return parsed


    @classmethod
    def save_normalized(
        cls,
        normalized: typing.Dict[str, typing.Tuple[str]],
        file: typing.Optional[os.PathLike] = None,
        *args,
        **kwargs
    ) -> os.PathLike:

        """Write out the results of normalize_item to a file."""
        _filepath = file or const.DEFAULT_NORMALIZED_SAVE_FILENAME
        import json
        saved = False

        with open(_filepath, "w") as dumpfile:
            json.dump(normalized, dumpfile, indent=4)

        saved = True
        return _filepath if saved else None


    @classmethod
    def transpose_columns(
        cls,
        data: typing.Dict[str, typing.Tuple[str]],
        *args, **kwargs
    ):
        """."""
        transposition_iterable = zip(data.keys(), *zip(*data.values()))
        transposed_data = []

        for row in transposition_iterable:
            key, values = row[0], row[1:]

            for (i, val) in enumerate(values):
                try:
                    curr_dict = transposed_data[i]

                except IndexError:
                    curr_dict = {}
                    transposed_data.append(curr_dict)

                curr_dict[key] = val

        return transposed_data


    @classmethod
    def retrieve_data_links(
        cls,
        normalized: typing.Dict[str, typing.Tuple[str]],
        data_headers: typing.Optional[typing.Iterable] = None,
        metadata_headers: typing.Optional[typing.Iterable] = None,
        *args, **kwargs
    ) -> dict[str, tuple]:
        """Parse normalized metadata columns and extract FTP links to data."""
        _metadata_headers = metadata_headers or ()
        _data_headers = data_headers

        if not _data_headers:
            _data_headers = set()
            search_qry = re.compile("Sample_supplementary_file.*")

            for header in normalized:
                if search_qry.fullmatch(header or ""):
                    _data_headers.add(header)

        titles = normalized["Sample_title"]
        links = {}

        for (i, title) in enumerate(titles):
            link_data = tuple(filter(None, (
                build_data_ftp_url(
                    normalized[link_header][i]
                )
                for link_header
                in _data_headers
            )))

            if link_data:
                links[title] = link_data

        return links

    @classmethod
    def save_data(cls, data: str, file: typing.Optional[os.PathLike] = None, *args, **kwargs) -> os.PathLike:
        """Write out the results of extract_item to a file."""
        _filepath = file or const.DEFAULT_EXTRACTED_SAVE_FILENAME
        saved = False

        with open(_filepath, "w") as dumpfile:
            dumpfile.write(data)

        saved = True
        return _filepath if saved else None

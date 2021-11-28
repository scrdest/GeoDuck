import itertools
import os
import typing

import app.constants as const
import app.utils.spark as sparkutils
from app.utils.decorators import preflight_message
from app.utils.logs import logger

SparkProcessingBackend = NotImplemented

if sparkutils.SPARK_SUPPORT and False:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.utils import AnalysisException
    from app.abcs import AbstractProcessingBackend
    from app.utils.registry import registry_entry
    from app.utils.ftp import ftp_listdir

    @registry_entry(as_key=const.BACKEND_SPARK, registry_key=const.DEFAULT_BACKEND_REGISTRY_KEY)
    class SparkProcessingBackend(AbstractProcessingBackend):
        """A backend using Apache Spark for processing the data."""

        @classmethod
        def save_extracted(cls, extracted, file: typing.Optional[os.PathLike] = None, *args, **kwargs) -> os.PathLike:
            """Write out the results of extract_item to a file."""


        @classmethod
        def save_normalized(cls, normalized, file: typing.Optional[os.PathLike] = None, *args, **kwargs) -> os.PathLike:
            """Write out the results of normalize_item to a file."""


        @classmethod
        def _load_handler(cls, session, source_path, csv_options):
            result = (
                session
                .read
                .csv(path=source_path, **csv_options)
            )
            return result


        @classmethod
        def load_csv(
            cls,
            files: typing.Iterable[typing.Tuple[str, DataFrame, str]],
            session: typing.Optional[SparkSession] = None,
            csv_options: typing.Optional[dict] = None,
            *args, **kwargs
        ) -> typing.Generator[typing.Tuple[str, DataFrame, DataFrame], None, int]:

            """A low-level workflow for extracting CSVs from NCBI GEO.

            As per standard Spark, this only builds the computational graph;
            the actual execution is deferred until the pipeline is triggered.

            :param baseaddr: Path to source directory on the remote FTP server
            :param files: An iterable of files in the source directory to load
            :param session: Optional; a cached SparkSession. Will be getOrCreated otherwise.

            :returns: A dict of <file in files> : <DataFrame of loaded data>
            """
            _session = session or sparkutils.get_spark_session()
            _csv_options = csv_options or {}

            processed_count = 0

            for src_fname, metadata_df, trg_fnames in files:
                _trg_fnames = (trg_fnames,) if isinstance(trg_fnames, str) else trg_fnames

                for source_path in _trg_fnames:
                    processed_count += 1

                    loader_func = preflight_message(f"Loading CSV from {source_path}...")(cls._load_handler)
                    datum_raw = loader_func(
                        session=session,
                        source_path=source_path,
                        csv_options=_csv_options
                    )

                    yield source_path, metadata_df, datum_raw

            return processed_count


        @classmethod
        def load_metadata(
            cls,
            baseaddr: str,
            files: typing.Iterable[typing.Tuple[str, str]],
            session: typing.Optional[SparkSession] = None,
            *args, **kwargs
        ) -> typing.Generator[typing.Tuple[str, DataFrame], None, int]:

            """First stage of processing - extracting metadata as a Matrix file.

            As per standard Spark, this only builds the computational graph;
            the actual execution is deferred until the pipeline is triggered.

            :param baseaddr: Path to source directory on the remote FTP server
            :param files: An iterable of files in the source directory to load
            :param session: Optional; a cached SparkSession. Will be getOrCreated otherwise.

            :returns: A dict of <file in files> : <DataFrame of loaded data>
            """
            _session = session or sparkutils.get_spark_session()
            metadata = None

            metadata_csv_options = dict(
                sep="\t"
            )

            file_urls = [
                (src_name, None, f'ftp://anonymous:anonymous@ftp.ncbi.nlm.nih.gov/{baseaddr}{trg_name}')
                for src_name, trg_name in files
            ]

            raw_metadata_loader = preflight_message(f"Loading metadata from {file_urls}")(cls.load_csv)(
                files=file_urls,
                session=session,
                csv_options=metadata_csv_options,
                *args,
                **kwargs
            )

            processed_count = 0

            for filename, _, dataframe in raw_metadata_loader:
                processed_count += 1
                logger.info(f"Processing metadata file: {filename}")
                yield filename, dataframe

            return processed_count


        @classmethod
        def extract_data_files_from_metadata(
            cls,
            metadata_stream: typing.Generator[typing.Tuple[str, DataFrame], None, int]
        ) -> typing.Generator[typing.Tuple[str, DataFrame], None, int]:

            processed = 0
            for src_fname, src_df in metadata_stream:
                processed += 1

                relevant_rows = (
                    src_df.filter(
                        src_df._c0.like('!Sample_supplementary_file')
                    )
                )

                raw_addresses = (
                    filtered_df
                    ._c1
                    .replace(
                        # inject dummy credentials; required by Spark
                        'ftp://ftp',
                        'ftp://anonymous:anonymous@ftp'
                    )
                    for filtered_df in relevant_rows.collect()
                    if filtered_df._c1
                )

                addresses = [addr for addr in raw_addresses if '.' in addr]
                yield src_fname, src_df, addresses

            return processed


        @classmethod
        def load_data(
            cls,
            baseaddr: str,
            metadata_files: typing.Optional[typing.Dict[str, DataFrame]] = None,
            metadata_stream: typing.Optional[typing.Generator[typing.Tuple[str, DataFrame], None, None]] = None,
            session: typing.Optional[SparkSession] = None,
            *args, **kwargs
        ) -> typing.Dict[str, DataFrame]:

            """Handles the Extract step of the processing - defines the steps
            to load data from the GEO database.

            As per standard Spark, this only builds the computational graph;
            the actual execution is deferred until the pipeline is triggered.

            :param baseaddr: Path to source directory on the remote FTP server
            :param files: An iterable of files in the source directory to load
            :param session: Optional; a cached SparkSession. Will be getOrCreated otherwise.

            :returns: A dict of <file in files> : <DataFrame of loaded data>
            """
            _session = session or sparkutils.get_spark_session()

            _chainable_streams = []
            if metadata_files:
                _chainable_streams.append(metadata_files.items())
            if metadata_stream:
                _chainable_streams.append(metadata_stream)

            _metadata_stream = itertools.chain(*_chainable_streams)

            data_files = preflight_message(
                f"Extracting data URLs from the metadata stream..."
                )(cls.extract_data_files_from_metadata)(
                    metadata_stream=_metadata_stream
                )

            raw_data = preflight_message(
                f"Loading data from FTP URLs..."
                )(cls.load_csv)(
                    files=data_files,
                    session=session,
                    *args,
                    **kwargs
                )

            data = {src_fname: (data_df, meta_df) for (src_fname, meta_df, data_df) in raw_data}
            _dbg_iter = iter(data.values())  # to inspect stuff in debugger neatly
            return data


        @classmethod
        def extract_item(cls, addr: str, fname: str, *args, **kwargs):
            """The main processing pipeline for a single source URL.

            :param addr: Path to the source FTP directory
            :param fname: Filename in the source FTP directory
            """
            matrix_metadata_files = [
                (matrixfile, matrixfile)
                for (matrixfile, metadata)
                in ftp_listdir(address=addr)
                if matrixfile and fname in matrixfile
            ]
            session = sparkutils.get_spark_session()
            metadata_loader = None
            try:
                metadata_loader = cls.load_metadata(
                    baseaddr=addr,
                    files=matrix_metadata_files,
                    session=session
                )
            except AnalysisException as AnEx: logger.exception(AnEx)

            data = None
            try: data = cls.load_data(baseaddr=addr, metadata_stream=metadata_loader, session=session)
            except AnalysisException as AnEx: logger.exception(AnEx)

            return data

        @classmethod
        def normalize_item(cls, extracted, *args, **kwargs) -> typing.Any:
            return extracted

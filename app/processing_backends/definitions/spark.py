import itertools

import constants as const
import app.utils.spark as sparkutils

if sparkutils.SPARK_SUPPORT:
    import typing
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.utils import AnalysisException
    from abcs import AbstractProcessingBackend
    from app.utils.registry import registry_entry
    from app.utils.ftp import ftp_listdir

    @registry_entry(as_key=const.BACKEND_SPARK, registry_key=const.DEFAULT_BACKEND_REGISTRY_KEY)
    class SparkProcessingBackend(AbstractProcessingBackend):
        """A backend using Apache Spark for processing the data."""


        @classmethod
        def load_csv(
            cls,
            files: typing.Iterable[typing.Tuple[str, str]],
            session: typing.Optional[SparkSession] = None,
            csv_options: typing.Optional[dict] = None,
            *args, **kwargs
        ) -> typing.Generator[typing.Tuple[str, DataFrame], None, int]:

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

            for src_fname, trg_fnames in files:
                _trg_fnames = (trg_fnames,) if isinstance(trg_fnames, str) else trg_fnames

                for source_path in _trg_fnames:
                    processed_count += 1

                    datum_raw = (
                        session
                        .read
                        .csv(path=source_path, **_csv_options)
                    )

                    yield source_path, datum_raw

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
                (src_name, f'ftp://anonymous:anonymous@ftp.ncbi.nlm.nih.gov/{baseaddr}{trg_name}')
                for src_name, trg_name in files
            ]

            raw_metadata_loader = cls.load_csv(
                files=file_urls,
                session=session,
                csv_options=metadata_csv_options,
                *args,
                **kwargs
            )

            processed_count = 0

            for filename, dataframe in raw_metadata_loader:
                processed_count += 1
                yield filename, dataframe

            return processed_count


        @classmethod
        def extract_data_files(
            cls,
            metadata_stream: typing.Generator[typing.Tuple[str, DataFrame], None, int]
        ) -> typing.Generator[typing.Tuple[str, DataFrame], None, int]:

            processed = 0
            for src_fname, src_df in metadata_stream:
                processed += 1

                relevant_rows = (
                    src_df.filter(
                        src_df._c0.contains('!Sample_supplementary_file')
                    )
                )

                addresses = [
                    (filtered_df
                     ._c1
                     .replace(
                        # inject dummy credentials; required by Spark
                        'ftp://ftp',
                        'ftp://anonymous:anonymous@ftp'
                     )
                    )
                    for filtered_df in relevant_rows.collect()
                    if filtered_df._c1
                ]
                yield src_fname, addresses

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

            data_files = cls.extract_data_files(metadata_stream=_metadata_stream)

            raw_data = cls.load_csv(
                files=data_files,
                session=session,
                *args,
                **kwargs
            )

            data = {src_fname: data_df for src_fname, data_df in raw_data}
            return data


        @classmethod
        def process_item(cls, addr: str, fname: str, *args, **kwargs):
            """The main processing pipeline for a single source URL.

            :param addr: Path to the source FTP directory
            :param fname: Filename in the source FTP directory
            """
            matrix_metadata_files = [
                (matrixfile, matrixfile)
                for (matrixfile, metadata) in ftp_listdir(address=addr)
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
            except AnalysisException as AnEx: print(AnEx)

            data = None
            try: data = cls.load_data(baseaddr=addr, metadata_stream=metadata_loader, session=session)
            except AnalysisException as AnEx: print(AnEx)

            return data

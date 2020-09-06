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
        def load_data(
            cls,
            baseaddr: str,
            files: typing.Iterable[str],
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

            raw_data = {}
            for fname in files:
                source_path = f'ftp://anonymous:anonymous@ftp.ncbi.nlm.nih.gov/{baseaddr}{fname}'

                datum_raw = (
                    session
                    .read
                    .csv(path=source_path)
                )

                raw_data[fname] = datum_raw

            return raw_data


        @classmethod
        def process_item(cls, addr: str, fname: str, *args, **kwargs):
            """The main processing pipeline for a single source URL.

            :param addr: Path to the source FTP directory
            :param fname: Filename in the source FTP directory
            """
            files = [csvfile for (csvfile, metadata) in ftp_listdir(address=addr) if '.csv' in csvfile]
            session = sparkutils.get_spark_session()
            data = None
            try: data = cls.load_data(baseaddr=addr, files=files, session=session)
            except AnalysisException as AnEx: print(AnEx)
            return data

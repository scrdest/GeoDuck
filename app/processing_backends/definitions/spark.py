import constants as const
import utils.spark as sparkutils

if sparkutils.SPARK_SUPPORT:
    from pyspark.sql.utils import AnalysisException
    from abcs import AbstractProcessingBackend
    from utils.registry import registry_entry
    from utils.ftp import ftp_listdir

    @registry_entry(as_key=const.BACKEND_SPARK, registry_key=const.DEFAULT_BACKEND_REGISTRY_KEY)
    class SparkProcessingBackend(AbstractProcessingBackend):
        @classmethod
        def load_data(cls, baseaddr, files, session=None, *args, **kwargs):
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
        def process_item(cls, addr, fname, *args, **kwargs):
            files = [csvfile for (csvfile, metadata) in ftp_listdir(address=addr) if '.csv' in csvfile]
            session = sparkutils.get_spark_session()
            data = None
            try: data = cls.load_data(baseaddr=addr, files=files, session=session)
            except AnalysisException as AnEx: print(AnEx)
            return data

import app.constants as const
from app.utils.misc import cache

SPARK_SUPPORT = False

try:
    import pyspark
    SPARK_SUPPORT = True

except ImportError as IEr:
    SPARK_SUPPORT = False


if SPARK_SUPPORT:
    from pyspark.sql import SparkSession

    @cache()
    def get_spark_session() -> SparkSession:
        session = SparkSession.builder.appName(const.APP_NAME).getOrCreate()
        return session

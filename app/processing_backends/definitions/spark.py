import constants as const
from utils.spark import SPARK_SUPPORT

if SPARK_SUPPORT:
    from utils.registry import registry_entry
    from abcs import AbstractProcessingBackend

    @registry_entry(as_key=const.BACKEND_SPARK, registry_key=const.DEFAULT_BACKEND_REGISTRY_KEY)
    class SparkProcessingBackend(AbstractProcessingBackend):
        @classmethod
        def process_item(cls, addr, fname, *args, **kwargs):
            pass

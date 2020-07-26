SPARK_SUPPORT = False

try:
    import pyspark
    SPARK_SUPPORT = True

except ImportError as IEr:
    SPARK_SUPPORT = False

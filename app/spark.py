"""
Contains entry points to SparkContext
"""
import findspark
import pyspark


class Context(object):
    """
    Entry point to SparkContext on remote YARN
    """

    def __enter__(self):
        findspark.init()
        conf = pyspark.SparkConf()
        conf.set('spark.master', 'yarn')
        conf.set('spark.master', 'yarn')
        conf.set('spark.executor.instances', 1)
        conf.set('spark.yarn.isPython', 'true')
        self.sc = pyspark.SparkContext(conf=conf)
        return self.sc

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sc.stop()

"""
Contains entry points to SparkContext
"""
import findspark
import pyspark
import pyspark.sql as sql

from constants import *

LOCAL = False


class Session(object):
    """
    Entry point to SparkSession on local or remote YARN
    """

    def __enter__(self):
        findspark.init()
        conf = pyspark.SparkConf()
        if LOCAL:
            pass
        else:
            conf.set('spark.master', 'yarn')
            conf.set('spark.master', 'yarn')
            conf.set('spark.executor.instances', 1)
            conf.set('spark.yarn.isPython', 'true')
        self.session = sql.SparkSession.builder.appName(APP_NAME).config(conf=conf).getOrCreate()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.stop()
        sql.SparkSession._instantiatedContext = None

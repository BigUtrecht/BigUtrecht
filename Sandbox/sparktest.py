from parquet.parquet import *

findspark.init()
conf = pyspark.SparkConf()
conf.set('spark.master', 'yarn')
conf.set('spark.master', 'yarn')
conf.set('spark.executor.instances', 1)
conf.set('spark.yarn.isPython', 'true')
spark = sql.SparkSession.builder.appName(APP_NAME).config(conf=conf).getOrCreate()
readResults(spark, "flow").show()
spark.stop()
sql.SparkSession._instantiatedContext = None
spark = sql.SparkSession.builder.appName(APP_NAME).config(conf=conf).getOrCreate()
readResults(spark, "flow").show()
spark.stop()

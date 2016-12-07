from sys import maxsize

import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from app.spark import *
from parquet import parquet


def createOverallBudget():
    with Session() as spark:
        flow = parquet.readResults(spark, "flow")
        flow.registerTempTable('flow')
        globalflow = spark.sql(
            "SELECT Timestamp, max(Datum) Datum, max(Tijd) Tijd, sum(Flow) Flow, sum(Volume) Volume FROM flow GROUP BY Timestamp ORDER BY Timestamp")
        budget = globalflow.select(globalflow.Timestamp, globalflow.Datum, globalflow.Tijd, globalflow.Flow,
                                   globalflow.Volume,
                                   F.sum(globalflow.Flow).over(
                                       Window.orderBy("Timestamp").rowsBetween(-maxsize, 0)).alias("Budget"))
        parquet.saveResults(spark, budget, 'budget')


if __name__ == '__main__':
    # createOverallBudget()
    with Session() as spark:
        budget = parquet.readResults(spark, 'budget')
        budget.registerTempTable('budget')
        pdbudget = spark.sql(
            "SELECT Datum, Budget FROM budget ORDER BY Timestamp").toPandas()
        pdbudget.plot(x="Datum", y="Budget", kind='line')
        plt.show()

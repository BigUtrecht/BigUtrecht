from sys import maxsize

import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from constants.spark import *
from etl import parquet


def createOverallBudget():
    """
    Overall budget analysis function
    Calculates the overall budget
    :return: None
    """
    with Session() as spark:
        flow = parquet.readResults(spark, "flow")
        flow.registerTempTable('flow')
        globalflow = spark.sql(
            "SELECT Timestamp, max(Datum) Datum, max(Tijd) Tijd, sum(Flow) Flow, sum(Volume) Volume FROM flow "
            "GROUP BY Timestamp ORDER BY Timestamp")
        budget = globalflow.select(globalflow.Timestamp, globalflow.Datum, globalflow.Tijd, globalflow.Flow,
                                   globalflow.Volume,
                                   F.sum(globalflow.Flow).over(
                                       Window.orderBy("Timestamp").rowsBetween(-maxsize, 0)).alias("Budget"))
        parquet.saveResults(spark, budget, 'overallbudget')


def createDailyBudget():
    """
    Daily budget analysis function
    Calculates the daily budget
    :return: None
    """
    with Session() as spark:
        flow = parquet.readResults(spark, "flow")
        flow.registerTempTable('flow')
        globalflow = spark.sql(
            "SELECT Timestamp, max(Datum) Datum, max(Tijd) Tijd, sum(Flow) Flow, sum(Volume) Volume FROM flow "
            "GROUP BY Timestamp ORDER BY Timestamp")
        budget = globalflow.select(globalflow.Timestamp, globalflow.Datum, globalflow.Tijd, globalflow.Flow,
                                   globalflow.Volume,
                                   F.sum(globalflow.Flow).over(
                                       Window.partitionBy("Datum").orderBy("Timestamp").rowsBetween(-maxsize, 0)).alias(
                                       "Budget"))
        parquet.saveResults(spark, budget, 'budget')

if __name__ == '__main__':
    # createOverallBudget()
    # createDailyBudget()
    with Session() as spark:
        budget = parquet.readResults(spark, 'budget')
        budget.registerTempTable('budget')
        pdbudget = spark.sql(
            "SELECT Tijd, avg(Flow) Flow, min(Flow) MinFlow, max(Flow) MaxFlow, avg(Budget) Budget FROM budget "
            "GROUP BY Tijd ORDER BY max(Timestamp)").toPandas()
        pdbudget.plot(x="Tijd", y=["Flow", "MinFlow", "MaxFlow"], kind='line', color=["green", "red", "blue"])
        plt.show()

from constants.spark import Session
from etl import parquet
from etl.parquet import readLocatie


def getLocations():
    with Session() as spark:
        locaties = readLocatie(spark)
        locaties.MeetpuntRichtingCode.collect()
        return locaties


def getFlowForLocation(location):
    with Session() as spark:
        flow = parquet.readResults(spark, "flow")
        flow.registerTempTable('flow')
        avgflow = spark.sql(
            "SELECT MeetpuntCode, max(Timestamp) Timestamp, Tijd, avg(Flow) Flow, avg(Inflow) Inflow, "
            "-avg(Outflow) Outflow FROM flow GROUP BY Tijd, MeetpuntCode")
        avgflow.registerTempTable('avgflow')
        pdflow = spark.sql(
            "SELECT Tijd, Flow, Inflow, Outflow FROM avgflow WHERE MeetpuntCode='%s' ORDER BY Timestamp" % location).toPandas()
        return pdflow

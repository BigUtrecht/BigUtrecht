import matplotlib.pyplot as plt

from app.spark import Session
from parquet import parquet


def createFlowFrame():
    """
    Dummy analysis function
    To be used as analysis template. Copy function body
    Prints minimum and maximum date
    :return: None
    """
    with Session() as spark:
        # spark is the SparkSession. It is the entry point for spark operations.
        # spark.sparkContext contains the SparkContext, if you really need it.
        telling = parquet.readTelling(spark)  # DataFrame containing telling data
        locatie = parquet.readLocatie(spark)
        telling.registerTempTable("telling")
        locatie.registerTempTable("locatie")
        tellingGroup = spark.sql("SELECT UniekeMeetpuntRichtingCode, "
                                 "max(Datum) Datum, "
                                 "max(Tijd) Tijd, T"
                                 "imestamp, "
                                 "sum(Intensiteit) Intensiteit "
                                 "FROM telling "
                                 "GROUP BY UniekeMeetpuntRichtingCode, Timestamp")
        tellingGroup.registerTempTable("tellingGroup")
        tellingJoin = spark.sql(
            "SELECT * FROM tellingGroup JOIN locatie AS l ON tellingGroup.UniekeMeetpuntRichtingCode=l.MeetpuntRichtingCode")
        tellingJoin.registerTempTable("tellingj")
        tellingIn = spark.sql("SELECT MeetpuntRichtingCode, "
                              "MeetpuntCode, "
                              "Datum, "
                              "Tijd, "
                              "Timestamp, "
                              "Intensiteit, "
                              "RichtingCode FROM tellingj "
                              "WHERE RichtingCode=1")
        tellingOut = spark.sql("SELECT MeetpuntRichtingCode, "
                               "MeetpuntCode, "
                               "Datum, "
                               "Tijd, "
                               "Timestamp, "
                               "Intensiteit, "
                               "RichtingCode FROM tellingj "
                               "WHERE RichtingCode=2")
        tellingIn.registerTempTable("tellingin")
        tellingOut.registerTempTable("tellingout")
        tellingFlow = spark.sql("SELECT in.MeetpuntCode MeetpuntCode, "
                                "in.Datum Datum, "
                                "in.Tijd Tijd, "
                                "in.Timestamp Timestamp, "
                                "in.Intensiteit Inflow, "
                                "out.Intensiteit Outflow, "
                                "(in.Intensiteit - out.Intensiteit) Flow, "
                                "(in.Intensiteit + out.Intensiteit) Volume FROM tellingin in "
                                "JOIN tellingout out ON in.MeetpuntCode=out.MeetpuntCode "
                                "AND in.Timestamp=out.Timestamp")
        parquet.saveResults(spark, tellingFlow, "flow")


if __name__ == '__main__':
    createFlowFrame()
    with Session() as spark:
        flow = parquet.readResults(spark, "flow")
        flow.registerTempTable('flow')
        avgflow = spark.sql(
            "SELECT MeetpuntCode, max(Timestamp) Timestamp, Tijd, avg(Flow) Flow FROM flow GROUP BY Tijd, MeetpuntCode")
        avgflow.registerTempTable('avgflow')
        pdflow = spark.sql(
            "SELECT Timestamp, Tijd, Flow FROM avgflow WHERE MeetpuntCode='1165' ORDER BY Timestamp").toPandas()
        pdflow.plot(x="Tijd", y="Flow", kind='bar')
        plt.show()

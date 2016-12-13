import matplotlib.pyplot as plt

from constants.spark import Session
from etl import parquet


def createDistributionGraph():
    """
    Distribution analysis function
    Calculates the distribution of cyclists over the different measurement points
    Shows the results in a bar plot
    :return: None
    """
    with Session() as spark:
        flow = parquet.readResults(spark, "flow")
        flow.registerTempTable("flow")
        locatie = parquet.readLocatie(spark)
        locatie.registerTempTable("locatie")
        meetpuntcodes = [str(i.MeetpuntCode) for i in
                         spark.sql("SELECT MeetpuntCode FROM locatie GROUP BY MeetpuntCode").collect()]
        meetpuntcolumns = {}
        map(lambda code: meetpuntcolumns.update({code: "flow_%s" % code}), meetpuntcodes)
        avgflow = spark.sql("SELECT Tijd, MeetpuntCode, avg(Flow) Flow "
                            "FROM flow GROUP BY Tijd, MeetpuntCode "
                            "ORDER BY max(Timestamp)").toPandas()
        groups = avgflow.groupby("MeetpuntCode")
        grouplist = {}
        map(lambda code: grouplist.update(
            {code: groups.get_group(code).rename(index=str, columns={"Flow": meetpuntcolumns[code]})}), meetpuntcodes)
        tijden = spark.sql("SELECT Tijd FROM flow GROUP BY Tijd").toPandas()
        for code in meetpuntcodes:
            tijden = tijden.join(grouplist[code], on="Tijd", rsuffix="_%s" % code)
        tijden.plot(x="Tijd", y=meetpuntcolumns.values(), kind="bar", stacked=False)
        plt.show()


if __name__ == '__main__':
    createDistributionGraph()

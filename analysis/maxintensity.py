from pyspark.sql.functions import *

from constants.spark import *
from etl import parquet
from visualisation import map


def maxPointAnalysis():
    """
    Maximum intensity analysis function
    Calculates the maximum intensity for each point
    Exports maximum intensity to a map
    :return: None
    """
    with Session() as s:
        # spark is the SparkSession. It is the entry point for spark operations.
        # spark.sparkContext contains the SparkContext, if you really need it.
        telling = parquet.readTelling(s)  # DataFrame containing telling data

        ## Dataframe with each vehicle type added together
        add_category = telling.groupBy(["UniekeMeetpuntRichtingCode", "Timestamp", "Tijd"]).\
            agg({"Intensiteit": "sum"}).orderBy("Timestamp")

        ## Filter all intensities of zero out of the dataframe
        filter_zero = add_category.filter(add_category["sum(Intensiteit)"] > 0)

        ## Select the maximum intensity for each point
        point_max = filter_zero.groupBy("UniekeMeetpuntRichtingCode").agg({"sum(Intensiteit)": "max"}).\
            orderBy("max(sum(Intensiteit))", ascending=False)

        ## Dataframe with max intensity per point
        max_total = filter_zero.join(point_max, (point_max["UniekeMeetpuntRichtingCode"] ==
                                                 filter_zero["UniekeMeetpuntRichtingCode"]) &
                                     (point_max["max(sum(Intensiteit))"] == filter_zero["sum(Intensiteit)"]),"inner")\
            .drop(filter_zero["UniekeMeetpuntRichtingCode"]).drop(point_max["max(sum(Intensiteit))"]).\
            withColumnRenamed("sum(Intensiteit)", "MaxIntensiteit")

        ## Show the data on a map
        map.map_dataframe(max_total, 'MaxIntensiteit', 1000, 500, 'Maximum intensity', '/tmp/map_max_intensity.html')

        parquet.saveResults(s, max_total, "maximum")

if __name__ == '__main__':
    maxPointAnalysis()

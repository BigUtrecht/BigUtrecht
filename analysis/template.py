from pyspark.sql.functions import *

from app.spark import *
from parquet import parquet
from visualisation import map


def dummyAnalysis():
    """
    Dummy analysis function
    To be used as analysis template. Copy function body
    Prints minimum and maximum date
    :return: None
    """
    with Session() as s:
        # spark is the SparkSession. It is the entry point for spark operations.
        # spark.sparkContext contains the SparkContext, if you really need it.
        telling = parquet.readTelling(s)  # DataFrame containing telling data

        # Load rush hour data
        #rush_time =
        #select_time = lambda t:

        # Calculate moments with highest intensity
        #intensity = telling.orderBy(telling.Intensiteit.desc()).show(40)
        #point_average = telling.groupBy("UniekeMeetpuntRichtingCode").avg("Intensiteit").orderBy("avg(Intensiteit)", ascending = False).show(34)
        #telling.show(5)

        ## Dataframe with each vehicle type added together
        add_category = telling.groupBy(["UniekeMeetpuntRichtingCode", "Timestamp", "Tijd"]).agg({"Intensiteit": "sum"}).orderBy("Timestamp", ascending = True)
        # add_category.show(50)

        filter_zero = add_category.filter(add_category["sum(Intensiteit)"] > 0)
        # #filter_zero.show(5)
        point_max = filter_zero.groupBy("UniekeMeetpuntRichtingCode").agg({"sum(Intensiteit)": "max"}).orderBy("max(sum(Intensiteit))", ascending=False)
        # #point_max.show(5)
        #
        # ## Dataframe with max intensity per point
        max_total = filter_zero.join(point_max, (point_max["UniekeMeetpuntRichtingCode"] == filter_zero["UniekeMeetpuntRichtingCode"]) & (point_max["max(sum(Intensiteit))"] == filter_zero["sum(Intensiteit)"]),"inner")\
            .drop(filter_zero["UniekeMeetpuntRichtingCode"]).drop(point_max["max(sum(Intensiteit))"]).withColumnRenamed("sum(Intensiteit)", "MaxIntensiteit")
        # max_total.show()
        #
        # add_category.registerTempTable("add_category")
        # filter_rush_morning = s.sql(
        #     "SELECT * FROM add_category WHERE Tijd IN ('08:00', '08:15', '08:30', '08:45', '09:00', '09:15', '09:30', '09:45', '10:00')")
        #
        # point_rush_morning_avg = filter_rush_morning.groupBy("UniekeMeetpuntRichtingCode").agg(
        #     {"sum(Intensiteit)": "avg"}).withColumnRenamed('avg(sum(Intensiteit))', 'MorningIntensity')
        # # point_rush_morning_avg.show(30)
        #
        # filter_rush_evening = s.sql(
        #     "SELECT * FROM add_category WHERE Tijd IN ('17:00', '17:15', '17:30', '17:45', '18:00', '18:15', '18:30', '18:45', '19:00')")
        #
        # point_rush_evening_avg = filter_rush_evening.groupBy("UniekeMeetpuntRichtingCode").agg(
        #     {"sum(Intensiteit)": "avg"}).withColumnRenamed('avg(sum(Intensiteit))', 'EveningIntensity')
        # # point_rush_evening_avg.show(30)
        #
        # point_rush_avg = point_rush_morning_avg.join(point_rush_evening_avg, (point_rush_morning_avg.UniekeMeetpuntRichtingCode == point_rush_evening_avg.UniekeMeetpuntRichtingCode)).\
        #     drop(point_rush_evening_avg.UniekeMeetpuntRichtingCode)
        # # point_rush_avg.show(34)
        #
        # point_rush_avg_in = point_rush_avg.filter(point_rush_avg.UniekeMeetpuntRichtingCode.endswith('1')).\
        #     withColumn('UniekeMeetpuntRichtingCode', regexp_replace('UniekeMeetpuntRichtingCode', '-1', '')).\
        #     withColumnRenamed('MorningIntensity', 'MorningIntensityIn').\
        #     withColumnRenamed('EveningIntensity', 'EveningIntensityIn')
        # point_rush_avg_out = point_rush_avg.filter(point_rush_avg.UniekeMeetpuntRichtingCode.endswith('2')).\
        #     withColumn('UniekeMeetpuntRichtingCode', regexp_replace('UniekeMeetpuntRichtingCode', '-2', '')).\
        #     withColumnRenamed('MorningIntensity', 'MorningIntensityOut').\
        #     withColumnRenamed('EveningIntensity', 'EveningIntensityOut')
        #
        #
        # point_rush_avg_in_out = point_rush_avg_in.join(point_rush_avg_out, (point_rush_avg_in.UniekeMeetpuntRichtingCode == point_rush_avg_out.UniekeMeetpuntRichtingCode)).\
        #     drop(point_rush_avg_out.UniekeMeetpuntRichtingCode)
        #
        # map.map_dataframe(point_rush_avg_in_out, 'MorningIntensityIn', 100, 50, 'Morning average intensity in',
        #                   '/tmp/map3.html',  'MorningIntensityOut', 'Morning average intensity out',
        #                   'EveningIntensityIn', 'Evening average intensity in', 'EveningIntensityOut', 'Evening average intensity out')
        #
        #
        #
        map.map_dataframe(max_total, 'MaxIntensiteit', 1000, 500, 'Maximum intensity', '/tmp/map1.html')
        #
        # map.map_dataframe()

        # Dummy code, creates RDD from datum column and stores it in a result parquet table
        #r = telling.select("Datum").rdd
        #parquet.saveResults(s, r, "dummyresult")


if __name__ == '__main__':
    dummyAnalysis()

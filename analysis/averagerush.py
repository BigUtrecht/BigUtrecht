from pyspark.sql.functions import *

from app.spark import *
from parquet import parquet
from visualisation import map


def avgRushAnalysis():
    """
    Average rush intensity analysis function
    Calculates the average intensity during morning and evening rush for each point
    Exports average intensities to a map
    :return: None
    """
    with Session() as s:
        # spark is the SparkSession. It is the entry point for spark operations.
        # spark.sparkContext contains the SparkContext, if you really need it.
        telling = parquet.readTelling(s)  # DataFrame containing telling data

        ## Dataframe with each vehicle type added together
        add_category = telling.groupBy(["UniekeMeetpuntRichtingCode", "Timestamp", "Tijd"]).\
            agg({"Intensiteit": "sum"}).orderBy("Timestamp", ascending = True)

        ## Filter all morning rush hours from the dataframe
        add_category.registerTempTable("add_category")
        filter_rush_morning = s.sql(
            "SELECT * FROM add_category WHERE Tijd IN ('08:00', '08:15', '08:30', '08:45', '09:00', "
            "'09:15', '09:30', '09:45', '10:00')")

        ## Calculate the average intensity during the morning rush
        point_rush_morning_avg = filter_rush_morning.groupBy("UniekeMeetpuntRichtingCode").\
            agg({"sum(Intensiteit)": "avg"}).withColumnRenamed('avg(sum(Intensiteit))', 'MorningIntensity')

        ## Filter all evening rush hours from the dataframe
        filter_rush_evening = s.sql(
            "SELECT * FROM add_category WHERE Tijd IN ('17:00', '17:15', '17:30', '17:45', '18:00', "
            "'18:15', '18:30', '18:45', '19:00')")

        ## Calculate the average intensity during the evening rush
        point_rush_evening_avg = filter_rush_evening.groupBy("UniekeMeetpuntRichtingCode").\
            agg({"sum(Intensiteit)": "avg"}).withColumnRenamed('avg(sum(Intensiteit))', 'EveningIntensity')

        ## Combine morning and evening rush intensity dataframes
        point_rush_avg = point_rush_morning_avg.join(point_rush_evening_avg,
                                                     (point_rush_morning_avg.UniekeMeetpuntRichtingCode ==
                                                      point_rush_evening_avg.UniekeMeetpuntRichtingCode)).\
            drop(point_rush_evening_avg.UniekeMeetpuntRichtingCode)

        ## Rename the columns to match morning and evening
        point_rush_avg_in = point_rush_avg.filter(point_rush_avg.UniekeMeetpuntRichtingCode.endswith('1')).\
            withColumn('UniekeMeetpuntRichtingCode', regexp_replace('UniekeMeetpuntRichtingCode', '-1', '')).\
            withColumnRenamed('MorningIntensity', 'MorningIntensityIn').\
            withColumnRenamed('EveningIntensity', 'EveningIntensityIn')
        point_rush_avg_out = point_rush_avg.filter(point_rush_avg.UniekeMeetpuntRichtingCode.endswith('2')).\
            withColumn('UniekeMeetpuntRichtingCode', regexp_replace('UniekeMeetpuntRichtingCode', '-2', '')).\
            withColumnRenamed('MorningIntensity', 'MorningIntensityOut').\
            withColumnRenamed('EveningIntensity', 'EveningIntensityOut')

        ## Combine the morning and evening rush intensity in one dataframe
        point_rush_avg_in_out = point_rush_avg_in.join(point_rush_avg_out,
                                                       (point_rush_avg_in.UniekeMeetpuntRichtingCode ==
                                                        point_rush_avg_out.UniekeMeetpuntRichtingCode)).\
            drop(point_rush_avg_out.UniekeMeetpuntRichtingCode)

        ## Show the data on a map
        map.map_dataframe(point_rush_avg_in_out, 'MorningIntensityIn', 100, 50, 'Morning average intensity in',
                          '/tmp/map_avg_rush.html',  'MorningIntensityOut', 'Morning average intensity out',
                          'EveningIntensityIn', 'Evening average intensity in',
                          'EveningIntensityOut', 'Evening average intensity out')

if __name__ == '__main__':
    avgRushAnalysis()

from app.spark import *
from parquet import parquet


def dummyAnalysis():
    """
    Dummy analysis function
    To be used as analysis template. Copy function body
    Prints minimum and maximum date
    :return: None
    """
    with Session() as spark:
        # spark is the SparkSession. It is the entry point for spark operations.
        # spark.sparkContext contains the SparkContext, if you really need it.
        telling = parquet.readTelling(s)  # DataFrame containing telling data

        # Dummy code, prints earliest and latest date
        print telling.agg({"Datum": "min"}).collect()
        print telling.agg({"Datum": "max"}).collect()

        # Dummy code, creates RDD from datum column and stores it in a result parquet table
        r = telling.select("Datum").rdd
        parquet.saveResults(s, r, "dummyresult")


if __name__ == '__main__':
    dummyAnalysis()

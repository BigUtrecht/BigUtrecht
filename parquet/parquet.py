from pydoop.hdfs import hdfs
from pyspark import RDD
from pyspark.sql import DataFrame

from constants.spark import *

TELLING = path.join(PARQUET_DIR, "telling")
LOCATIE = path.join(PARQUET_DIR, "locatie")
RESULT_DIR = path.join(PARQUET_DIR, "results")
h = hdfs()
if not h.exists(PARQUET_DIR):
    h.create_directory(PARQUET_DIR)

if not h.exists(RESULT_DIR):
    h.create_directory(RESULT_DIR)


def saveTelling(dataframe):
    """
    Appends DataFrame to telling parquet table
    :param DataFrame: the DataFrame to append
    :return: None
    """
    dataframe.write.parquet(TELLING, mode="overwrite")


def readTelling(session):
    """
    Returns a DataFrame containing the telling data stored as a parquet table
    :param session: SparkSession to be used for this DataFrame
    :return: the DataFrame
    """
    return session.read.parquet(TELLING)


def clearTelling():
    """
    Removes the telling parquet table, if it exists
    :return: None
    """
    h = hdfs()
    if h.exists(TELLING):
        h.delete(TELLING)


def saveLocatie(dataframe):
    """
    Appends DataFrame to locatie parquet table
    :param DataFrame: the DataFrame to append
    :return: None
    """
    dataframe.write.parquet(LOCATIE, mode="overwrite")


def readLocatie(session):
    """
    Returns a DataFrame containing the locatie data stored as a parquet table
    :param session: SparkSession to be used for this DataFrame
    :return: the DataFrame
    """
    return session.read.parquet(LOCATIE)


def clearLocatie():
    """
    Removes the locatie parquet table, if it exists
    :return: None
    """
    h = hdfs()
    if h.exists(LOCATIE):
        h.delete(LOCATIE)


def saveResults(session, data, name, mode="overwrite"):
    """
    Saves a DataFrame or RDD in a parquet table with given name. Overwrites by default
    :param data: the data to be stored
    :param name: the name of the result table
    :param mode: the saveMode
    :return: None
    """
    if isinstance(data, DataFrame):
        dataframe = data
    elif isinstance(data, RDD):
        dataframe = session.createDataFrame(data)
    else:
        raise ValueError("Data is DataFrame nor RDD.")
    p = path.join(RESULT_DIR, name)
    dataframe.write.parquet(p, mode=mode)


def readResults(session, name):
    """
    Reads a result parquet table with given name into a DataFrame
    :param session: the SparkSession to use with this DataFrame
    :param name: the target result parquet table name
    :return: the DataFrame
    """
    p = path.join(RESULT_DIR, name)
    return session.read.parquet(p)


def clearResults(name=""):
    """
    Clears target result parquet table name, or all result parquet tables if no name is given
    :param name: the target result parquet table name
    :return: None
    """
    p = path.join(RESULT_DIR, name)
    h = hdfs()
    if h.exists(p):
        h.delete(p)

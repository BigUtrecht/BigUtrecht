from pydoop.hdfs import hdfs

from app.spark import *

TELLING = path.join(PARQUET_DIR, "telling")
h = hdfs()
if not h.exists(PARQUET_DIR):
    h.create_directory(PARQUET_DIR)


def saveTelling(dataframe):
    dataframe.write.parquet(TELLING, mode="append")


def readTelling(session):
    return session.read.parquet(TELLING)


def clearTelling():
    h = hdfs()
    if h.exists(TELLING):
        h.delete(path.join(TELLING))

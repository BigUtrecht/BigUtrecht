import os.path as path

HDFS_URL = 'hdfs://scomp1334:9000'
BASE_DIR = path.join(HDFS_URL, "/user/groups/BigUtrecht")
TEMP_DIR = path.join(BASE_DIR, "tempfiles")
PARQUET_DIR = path.join(BASE_DIR, "parquet")

APP_NAME = "Big_Utrecht"

HOME_DIR = path.dirname(path.dirname(path.abspath(path.realpath(__file__))))

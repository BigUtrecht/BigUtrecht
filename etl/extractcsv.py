"""
Script that contains functions for downloading and extracting zip files and csv's
"""
import shutil
import urllib
from os import path, makedirs
from zipfile import ZipFile

import pandas as pd
import pydoop.hdfs as hdfs
from pyspark import SQLContext

import app.spark
from constants.constants import *


def downloadZiptoTempDir(location, tmpdir):
    """
    Downloads a zipfile to a local temporary directory, unpacks it and moves it to HDFS tempfile location
    :param location: the url containing the zip file
    :param tmpdir: temporary local folder to store and unpack zip
    :return whether the extraction succeeded
    """
    try:
        if not path.exists(tmpdir):
            makedirs(tmpdir)
        zipname = path.basename(location)
        localzipfile = path.join(tmpdir, zipname)
        urllib.urlretrieve(location, localzipfile)
        zipf = ZipFile(localzipfile)
        filelist = zipf.namelist()
        zipf.extractall(tmpdir)
        for f in filelist:
            localf = 'file://' + path.abspath(path.join(tmpdir, f))
            loc = path.join(TEMP_DIR, f)
            copyFileToHDFSFolder(localf, loc)
        return True
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except Exception, e:
        print e.__class__, e.message
        return False


def copyFileToHDFSFolder(localpath, hdfspath):
    """
    Copies a file from a local or HDFS to an HDFS location
    :param localpath: path to local file
    :param hdfspath: path to target file on HDFS
    :return: None
    """
    if localpath.startswith('file:/'):
        lf = hdfs.hdfs("", 0)
    else:
        lf = hdfs.hdfs()
    h = hdfs.hdfs()
    lf.copy(localpath, h, hdfspath)


def retrieveSources(sourcesfile='../datalocations.txt'):
    """
    Maps a file with data source locations and uses downloadZiptoTempDir to download and extract these
    :param sourcesfile: path to a local file containing the urls of source data
    :return: None
    """
    tmppath = './.tmp'
    if not path.exists(tmppath):
        makedirs(tmppath)
    all = True
    with open(sourcesfile) as f:
        for line in f:
            all = all and downloadZiptoTempDir(line, tmppath)
    shutil.rmtree(tmppath)
    return all


def removeNonAscii(string):
    """
    Removes non-ascii characters from a unicode string
    :param string: (unicode) string
    :return: stripped string
    """
    news = ""
    for s in string:
        if 0 <= ord(s) < 127: news += s
    return news


def createDataFrames():
    """
    Converts temporary CSV files to dataframes using pandas
    :return: a list of SQL Dataframes
    """
    h = hdfs.hdfs()
    filelist = map(lambda f: f['path'], h.list_directory(TEMP_DIR))
    frames = []
    with app.spark.Context() as sc:
        sql_sc = SQLContext(sc)
        for filename in filelist:
            with h.open_file(filename) as f:
                p = pd.read_csv(f, sep=";")
                p.columns = [removeNonAscii(c) for c in p.columns]
                print p.columns
                s_df = sql_sc.createDataFrame(p)
                frames.append(s_df)
    return frames


if __name__ == '__main__':
    print createDataFrames()[0]

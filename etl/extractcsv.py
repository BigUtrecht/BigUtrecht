"""
Script that contains functions for downloading and extracting zip files and csv's
"""
import shutil
import urllib
from datetime import datetime
from os import path, makedirs
from zipfile import ZipFile

import pydoop.hdfs as H

from etl.parquet import *


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
        lf = H.hdfs("", 0)
    else:
        lf = H.hdfs()
    h = H.hdfs()
    lf.copy(localpath, h, hdfspath)


def retrieveSources(sourcesfile='datalocations.txt'):
    """
    Maps a file with data source locations and uses downloadZiptoTempDir to download and extract these
    :param sourcesfile: path to a local file containing the urls of source data
    :return: None
    """
    tmppath = '.tmp'
    tmppath = path.abspath(tmppath)
    print tmppath
    if not path.exists(tmppath):
        makedirs(tmppath)
    all = True
    sourcesfile = path.abspath(sourcesfile)
    print sourcesfile
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


def createLocatieDataFrame(session, end="_Locatie"):
    """
    Creates a location Dataframe from the locatie csv files.
    :param session: SparkSession
    :param end: Suffix to filter CSV files on
    :return: a Dataframe containing telling locations
    """
    p = path.join(TEMP_DIR, '*%s.csv' % end)
    rdd = session.sparkContext.textFile(p).map(lambda line: line.split(';'))

    header = rdd.first()
    header_new = [header[1], header[3], header[4], header[5], "XRD", "YRD"]

    rdd_data = rdd.filter(lambda l: l != header).map(
        lambda l: [str(l[1]), str(l[3]), str(l[4], ), str(l[5]), str(l[11]), str(l[12])])
    header_str = ()
    for i in range(len(header_new)):
        header_str += tuple([str(header_new[i])])

    frame = session.createDataFrame(rdd_data, header_str)
    frame.registerTempTable("locatietemp")
    frame = session.sql("SELECT MeetpuntRichtingCode, "
                        "max(StraatNaamWegVak) StraatNaamWegVak, "
                        "max(MeetpuntCode) MeetpuntCode, "
                        "max(RichtingCode) RichtingCode, "
                        "max(XRD) XRD, "
                        "max(YRD) YRD "
                        "FROM locatietemp "
                        "WHERE MeetpuntCode<>'1160' AND MeetpuntCode<>'1161' "
                        "GROUP BY MeetpuntRichtingCode ")
    # Add missing points manually
    frame.registerTempTable("locatietemp")
    okframe = session.sql("SELECT MeetpuntRichtingCode, "
                          "StraatNaamWegVak, "
                          "MeetpuntCode, "
                          "RichtingCode, "
                          "CAST(XRD AS int), "
                          "CAST(YRD AS int) "
                          "FROM locatietemp "
                          "WHERE MeetpuntCode NOT IN ('1156', '1157', '1153')")
    frame1156 = session.sql(
        "SELECT MeetpuntRichtingCode, StraatNaamWegVak, MeetpuntCode, RichtingCode, 134144 AS XRD, 458090 AS YRD "
        "FROM locatietemp WHERE MeetpuntCode='1156'")
    frame1157 = session.sql(
        "SELECT MeetpuntRichtingCode, StraatNaamWegVak, MeetpuntCode, RichtingCode, 134772 AS XRD, 457492 AS YRD "
        "FROM locatietemp WHERE MeetpuntCode='1157'")
    frame1153 = session.sql(
        "SELECT MeetpuntRichtingCode, StraatNaamWegVak, MeetpuntCode, RichtingCode, 134944 AS XRD, 455428 AS YRD "
        "FROM locatietemp WHERE MeetpuntCode='1153'")
    frame = okframe.unionAll(frame1156).unionAll(frame1157).unionAll(frame1153)
    return frame


def createTellingDataFrame(session, end='_T'):
    """
    Converts temporary CSV files to dataframes
    :return: a list of SQL Dataframes
    """
    p = path.join(TEMP_DIR, '*%s.csv' % end)
    rdd = session.sparkContext.textFile(p).map(lambda line: line.split(';'))
    header = rdd.first()
    rdd_data = rdd.filter(lambda l: l != header).map(lambda l: [str(l[0]), str(l[1]), str(l[2]), str(l[3]), int(l[4])])
    header_str = ()
    for i in range(len(header)):
        header_str += tuple([str(header[i])])

    frame = session.createDataFrame(rdd_data, header_str)
    frame.registerTempTable("tellingtemp")
    session.udf.register("timestamp", lambda d, t: long(
        (datetime.strptime('%s %s' % (d, t), "%d-%m-%y %H:%M") - datetime(1970, 1, 1)).total_seconds()))
    frame = session.sql("SELECT *, timestamp(Datum, Tijd) Timestamp FROM tellingtemp")
    return frame


def etl():
    with Session() as spark:
        print retrieveSources()
        frame = createTellingDataFrame(spark)
        saveTelling(frame)
        frame = createLocatieDataFrame(spark)
        saveLocatie(frame)

if __name__ == '__main__':
    with Session() as session:
        print retrieveSources()
        frame = createTellingDataFrame(session)
        saveTelling(frame)
        frame = createLocatieDataFrame(session)
        saveLocatie(frame)
        locatie = readLocatie(session)
        locatie.show()
        telling = readTelling(session)
        telling.show(5)
        print telling.agg({"Datum": "min"}).collect()
        print telling.agg({"Intensiteit": "max"}).collect()

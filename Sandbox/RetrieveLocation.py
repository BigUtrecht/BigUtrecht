import pyspark
from pyspark.sql import SQLContext

sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)

rdd = sc.textFile('hdfs://scomp1334:9000/user/groups/BigUtrecht/tempfiles/UTREC_F_2015_jun_Locatie.csv').\
    map(lambda line: line.split(';'))

header = rdd.first()
header_new = [header[0], header[1], header[5], "XcoordinaatRD", "YcoordinaatRD", header[16], header[17], header[18],
              header[19], header[20], header[21], header[28]]

rdd_data = rdd.filter(lambda l: l != header).map(lambda l: [str(l[0]), str(l[1]), str(l[5]), str(l[11]), str(l[12]),
                                                            int(l[16]), int(l[17]), str(l[18]), str(l[19]), str(l[20]),
                                                            str(l[21]), str(l[28])])
header_str = ()
for i in range(len(header_new)):
    header_str += tuple([str(header_new[i])])

df = sqlContext.createDataFrame(rdd_data, header_str)

print df.show()

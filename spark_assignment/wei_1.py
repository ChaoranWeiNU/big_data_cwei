from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sc.setLogLevel("ERROR")
crime = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/cwei/crime")

from pyspark.sql.functions import *
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
crime = crime.map(lambda x: x.split(",")).map(lambda x: x[:21] )
header = crime.first()
header.append('month')
crime = crime.map(lambda x: x + [x[2][:2]]).filter(lambda x: x[0] != 'ID')
crime_df = crime.toDF(header)

tmp = crime_df.select("Year","month","ID")
histogram = tmp.filter(tmp['Year'] < 3000).filter(tmp['Year'] > 2000).groupBy("Year","month").count().groupBy("month").avg('count')

histogram = histogram.toPandas()

histogram.to_csv('histogram.csv')


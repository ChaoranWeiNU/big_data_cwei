from pyspark.sql import HiveContext
import pandas as pd
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

#time of the day

crime_time = crime.map(lambda x: (x[2][11:13] + x[2][20:22], 1))
crime_time_pattern = crime_time.reduceByKey(lambda a,b: a+b)
crime_time_pattern_result = sorted(crime_time_pattern.collect(), key = lambda x: x[1])
df = pd.DataFrame(crime_time_pattern_result)
df.columns = ['time of day','number of crime']

#month
crime_month = crime.map(lambda x: (x[-1],1))
crime_month_pattern = crime_month.reduceByKey(lambda a,b: a+b)
crime_month_pattern_result = sorted(crime_month_pattern.collect(), key = lambda x: x[1])
df = pd.DataFrame(crime_month_pattern_result)
df.columns = ['month','number of crime']


#day of week
import datetime
def date2weekday(string):
	string = string.split()[0]
	month, day, year = (int(x) for x in string.split('/'))    
	ans = datetime.date(year, month, day)
	return ans.strftime("%A")

crime_week = crime.map(lambda x: (date2weekday(x[2]),1))
crime_week_pattern = crime_week.reduceByKey(lambda a,b: a+b)
crime_week_pattern_result = sorted(crime_week_pattern.collect(), key = lambda x: x[1])
df = pd.DataFrame(crime_week_pattern_result)
df.columns = ['day of week','number of crime']





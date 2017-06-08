from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sc.setLogLevel("ERROR")
crime = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/cwei/crime")

'''
columns: 
'ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,
Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location'
'''

from pyspark.sql.functions import *
crime = crime.map(lambda x: x.split(",")).map(lambda x: x[:21] )
crime = crime.map(lambda x: x + [x[2][:2]]).filter(lambda x: x[0] != 'ID')

#(1)
crime_last_3_years = crime.filter(lambda x: x[17] in ['2015','2014','2013'])
crime_by_block = crime_last_3_years.map(lambda x: (x[3], 1)).reduceByKey(lambda a, b: a+b)
top_10_crim_by_block = crime_by_block.takeOrdered(10, key = lambda x: -x[1])


#(2)
def impute_year(List):
	years = [i[0] for i in List]
	if '2011' not in years:
		List.append(('2011',0))
	if '2012' not in years:
		List.append(('2012',0))
	if '2013' not in years:
		List.append(('2013',0))
	if '2014' not in years:
		List.append(('2014',0))
	if '2015' not in years:
		List.append(('2015',0))	
	List.sort(key = lambda x: int(x[0]))
	return List


crime_last_5_years = crime.filter(lambda x: x[17] in ['2013','2014','2015','2012','2011'])
crime_by_year_beats = crime_last_5_years.map(lambda x: ((x[10], x[17]),1)).reduceByKey(lambda a,b: a+b).map(lambda x: (x[0][0],[(x[0][1], x[1])]))
crime_by_beats = crime_by_year_beats.reduceByKey(lambda a,b: a+b).map(lambda x: (x[0], impute_year(x[1]))).map(lambda x: (x[0],[i[1] for i in x[1]]))
crime_cartesian = crime_by_beats.cartesian(crime_by_beats).filter(lambda x: x[0][0] < x[1][0]) #cartesian product to get pairs of data to compute correlation
from scipy.stats.stats import pearsonr    #didnt use mllib package because transform vector of length 5 to RDD is highly inefficient. 
crime_correlation = crime_cartesian.map(lambda a: ((a[0][0], a[1][0]), pearsonr(a[0][1],a[1][1])[0]))  
crime_corr_sorted = crime_correlation.sortBy(lambda a: a[1], ascending=False)
crime_corr_sorted.take(50)


#(3)
def Daly_bool(x):
	date = x[2].split("/")
	year = int(x[17])
	month = int(date[0])
	return year >= 1989 and year < 2011 or (year == 2011 and month <= 5)

def Emanuel_bool(x):
    date = x[2].split("/")
    year = int(x[17])
    month = int(date[0])
    return year > 2011 or (year == 2011 and month >= 5)

Daly = crime.filter(lambda x: len(x[17]) == 4 and x[17][0] == '2').filter(lambda x: Daly_bool(x))
Emanuel = crime.filter(lambda x: len(x[17]) == 4 and x[17][0] == '2').filter(lambda x: Emanuel_bool(x))

Daly_count = Daly.map(lambda x: (x[13], 1)).reduceByKey(lambda a,b: a+b)
Emanuel_count = Emanuel.map(lambda x: (x[13], 1)).reduceByKey(lambda a,b: a+b)

num_days_daly = Daly.map(lambda x: x[2][0:10]).distinct().count()
num_days_emanuel = Emanuel.map(lambda x: x[2][0:10]).distinct().count()
#normalize
Daly_count_norm = Daly_count.map(lambda x: (x[0], x[1]/num_days_daly))
Emanuel_count_norm = Emanuel_count.map(lambda x: (x[0],x[1]/num_days_emanuel))

Daly_result = pd.DataFrame(Daly_count_norm.collect())
Daly_result.columns = ['community area','number of crime']
Emanuel_result = pd.DataFrame(Emanuel_count_norm.collect())
Emanuel_result.columns = ['community area','number of crime']
Daly_result = Daly_result.sort_values(by = 'community area')
Emanuel_result = Emanuel_result.sort_values(by = 'community area')

Daly_result.set_index('community area', inplace=True)
Emanuel_result.set_index('community area', inplace=True)

joined_result = Daly_result.join(Emanuel_result,lsuffix='_Daly', rsuffix='_Emanuel')
joined_result
#conclusion: Emanuel kicks Daly's ass

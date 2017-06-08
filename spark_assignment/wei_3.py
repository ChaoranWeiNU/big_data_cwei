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
crime = crime.map(lambda x: x + [x[2][:2]]).filter(lambda x: x[0] != 'ID').filter(lambda x: x[4].isdigit())
crime_df = crime.toDF(header)

#join with external data: temperature of Chicago area over the years
temperature = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/cwei/temperature/temperature.csv")
temperature = temperature.map(lambda x: x.split(",")).map(lambda x: (x[2], x[1])).filter(lambda x: x[0] != 'year').persist()
temperature.take(10)
temperature_df = temperature.toDF(['Date', 'temperature'])
crime_df1 = crime_df.withColumn('Date', crime_df.Date.substr(0,10))
crime_join = (crime_df1.join(
	temperature_df, crime_df1.Date == temperature_df.Date,'inner').drop(temperature_df.Date))

#remove useless columns
crime_subset = crime_join.select("ID", "Date", "temperature","month","IUCR",'Beat')
#crime_subset.show()


#group by week
import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import *


def date2week_number(date):
    month, day, year = (int(x) for x in date.split('/'))   
    week_num = datetime.date(year, month, day).isocalendar()
    result = str(week_num[0]) + str(week_num[1])
    if len(result) == 5:
    	return result[:4] + '0' + result[4:]
    else:
    	return result


udfdate2week_number = udf(date2week_number, StringType())

crime_subset = crime_subset.withColumn('week_num', udfdate2week_number(crime_subset.Date))
crime_subset = crime_subset.drop('Date')
crime_by_week = crime_subset.groupBy("week_num","Beat").agg({"ID":"count", "temperature": "avg", "IUCR":"avg"})
#crime_by_week.show()


#sort data by week and lag
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window
crime_by_week = crime_by_week.sort(col("week_num"))
w = Window().partitionBy(col("Beat")).orderBy(col("week_num")) #partitionBy: generate window for each beat
crime_by_week = crime_by_week.select("*", lag("count(ID)").over(w).alias("crime_lag1")).na.drop()
crime_by_week = crime_by_week.select("*", lag("crime_lag1").over(w).alias("crime_lag2")).na.drop()
crime_by_week = crime_by_week.select(col('count(ID)').alias("label"), col('avg(temperature)').alias('temperature'), col('avg(IUCR)').alias('IUCR'),
	'crime_lag1', 'crime_lag2','Beat')
crime_by_week = crime_by_week.withColumn("label", crime_by_week["label"].cast(DoubleType())).persist()

#creating dummy variables
Beats = crime_by_week.select("Beat").distinct().rdd.flatMap(lambda x: x).collect()

exprs = [pyspark.sql.functions.when(pyspark.sql.functions.col("Beat") == category, 1).otherwise(0).alias(category)
         for category in Beats]

crime_by_week_dummy = crime_by_week.select("*", *exprs)
crime_by_week_dummy = crime_by_week_dummy.drop('Beat')


#standardize, label
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression,RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

feature_names =crime_by_week_dummy.schema.names[1:]
assembler = VectorAssembler(inputCols=['temperature','crime_lag1','crime_lag2', 'IUCR'],outputCol='allFeatures')
scaler = StandardScaler(inputCol='allFeatures',outputCol='features',withStd=True,withMean=False)
ols  = LinearRegression(maxIter=10)
pipeline = Pipeline(stages=[assembler,scaler, ols])

#for cross validation: https://mapr.com/blog/churn-prediction-pyspark-using-mllib-and-ml-packages/

paramGrid = ParamGridBuilder().addGrid(ols.regParam, [0.001,0.01,0.1]).build()
evaluator = RegressionEvaluator(labelCol = 'label', predictionCol='prediction', metricName='r2')   

crossval = CrossValidator(estimator = pipeline,
	                      estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=3)

CV_model = crossval.fit(crime_by_week)

best = CV_model.bestModel.stages[2]

transformed_data = CV_model.transform(crime_by_week)

print('r2:', evaluator.evaluate(transformed_data))

predictions = transformed_data.select('label', 'prediction')
predictions.toPandas().head()

#random forest implementation
rf = RandomForestRegressor(numTrees=10)
assembler = VectorAssembler(inputCols=['temperature','crime_lag1','crime_lag2', 'IUCR'],outputCol='features')
pipeline_rf = Pipeline(stages=[assembler, rf]) #tree-based algorithms do not need to scale
gridRF = ParamGridBuilder().addGrid(rf.maxDepth, [3,5,10]).build()
evaluator = RegressionEvaluator(labelCol = 'label', predictionCol='prediction', metricName='r2')   
crossval = CrossValidator(estimator = pipeline_rf,
	                      estimatorParamMaps=gridRF,
                          evaluator=evaluator,
                          numFolds=3)

CV_model = crossval.fit(crime_by_week)

best = CV_model.bestModel.stages[1]

transformed_data = CV_model.transform(crime_by_week)
print('r2:', evaluator.evaluate(transformed_data))

#prediction result
predictions = transformed_data.select('label', 'prediction')
predictions.toPandas().head()


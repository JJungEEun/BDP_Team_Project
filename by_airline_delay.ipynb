%spark2.pyspark
from pyspark.sql.functions import *
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

df=spark.read.csv("hdfs:///user/maria_dev/pro/2008resultfull.csv", header=True, inferSchema=True)

##///노선별 실제와 예상 비행시간 차이
result_df = df.groupBy("Origin","Dest").agg(F.mean("ElapsedTimeDelay").alias("ETDM"))
z.show(result_df)

##//운항 노선 갯수,딜레이 출발지별
result_df = df.groupBy("Origin").agg(F.count("ElapsedTimeDelay").alias("ETDC")).sort("ETDC")
result_df1 = df.groupBy("Origin").agg(F.mean("ElapsedTimeDelay").alias("ETDM")).sort("ETDM")

result_df2 = result_df.join(result_df1,result_df1["Origin"]==result_df["Origin"])

result_df2 = result_df2.sort("ETDC")

z.show(result_df2)
##//운항 노선 갯수,딜레이 도착지별
result_df = df.groupBy("Dest").agg(F.count("ElapsedTimeDelay").alias("ETDC")).sort("ETDC")
result_df1 = df.groupBy("Dest").agg(F.mean("ElapsedTimeDelay").alias("ETDM")).sort("ETDM")

result_df2 = result_df.join(result_df1,result_df1["Dest"]==result_df["Dest"])

result_df2 = result_df2.sort("ETDC")

z.show(result_df2)




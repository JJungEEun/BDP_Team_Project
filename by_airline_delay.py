%spark2.pyspark

from pyspark.sql.functions import *
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

df_2008=spark.read.csv("hdfs:///user/maria_dev/pro/2008resultfull.csv", header=True, inferSchema=True)
df_2007 = spark.read.csv("hdfs:///user/maria_dev/pro/2008resultfull.csv", header=True, inferSchema=True)
df_2006 = spark.read.csv("hdfs:///user/maria_dev/pro/2008resultfull.csv", header=True, inferSchema=True)
df_2005 = spark.read.csv("hdfs:///user/maria_dev/pro/2008resultfull.csv", header=True, inferSchema=True)
df_2004 = spark.read.csv("hdfs:///user/maria_dev/pro/2008resultfull.csv", header=True, inferSchema=True)
df_2003 = spark.read.csv("hdfs:///user/maria_dev/pro/2008resultfull.csv", header=True, inferSchema=True)
df_airline = df_2008 df_2003.union(df_2004).union(df_2005).union(df_2006).union(df_2007).union(df_2008)

df_airline = df_airline.filter(df_airline["Cancelled"]==0).filter(df_airline["Diverted"]==0)
df_airline = df_airline.replace(["NA"], ["0"], "CarrierDelay").replace(["NA"], ["0"], "WeatherDelay")
df_airline = df_airline.replace(["NA"], ["0"], "NASDelay").replace(["NA"], ["0"], "SecurityDelay").replace(["NA"], ["0"], "LateAircraftDelay")
df_airline = df_airline.withColumn("ElapsedTime", df_airline.ArrTime - df_airline.DepTime)
df_airline = df_airline.withColumn("CRSElapsedTime", df_airline.CRSArrTime - df_airline.CRSDepTime)
df_airline = df_airline.withColumn("ElapsedTimeDelay", df_airline.CRSElapsedTime - df_airline.ActualElapsedTime)
df_airline = df_airline.drop("DayOfWeek", "FlightNum", "Distance", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode")


##///노선별 실제와 예상 비행시간 차이
df = df_airline
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




%spark2.pyspark
from pyspark.sql.functions import *
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
#######추발 시간대별 시간 딜레이 정도
df_weather = spark.read.csv("hdfs:///user/maria_dev/pro/pre2008weather.csv", header=True, inferSchema=True) 

df_2008 = spark.read.csv("hdfs:///user/maria_dev/pro/2008resultfull.csv", header=True, inferSchema=True)
df_2007 = spark.read.csv("hdfs:///user/maria_dev/pro/2007resultfull.csv", header=True, inferSchema=True)
df_2006 = spark.read.csv("hdfs:///user/maria_dev/pro/2006resultfull.csv", header=True, inferSchema=True)
df_2005 = spark.read.csv("hdfs:///user/maria_dev/pro/2005resultfull.csv", header=True, inferSchema=True)
df_2004 = spark.read.csv("hdfs:///user/maria_dev/pro/2004resultfull.csv", header=True, inferSchema=True)
df_2003 = spark.read.csv("hdfs:///user/maria_dev/pro/2003resultfull.csv", header=True, inferSchema=True)

df_airline = df_2003.union(df_2004).union(df_2005).union(df_2006).union(df_2007).union(df_2008)

df_airline = df_airline.filter(df_airline["Cancelled"]==0).filter(df_airline["Diverted"]==0)
df_airline = df_airline.replace(["NA"], ["0"], "CarrierDelay").replace(["NA"], ["0"], "WeatherDelay")
df_airline = df_airline.replace(["NA"], ["0"], "NASDelay").replace(["NA"], ["0"], "SecurityDelay").replace(["NA"], ["0"], "LateAircraftDelay")
df_airline = df_airline.withColumn("ElapsedTime", df_airline.ArrTime - df_airline.DepTime)
df_airline = df_airline.withColumn("CRSElapsedTime", df_airline.CRSArrTime - df_airline.CRSDepTime)
df_airline = df_airline.withColumn("ElapsedTimeDelay", df_airline.CRSElapsedTime - df_airline.ActualElapsedTime)
df_airline = df_airline.drop("DayOfWeek", "FlightNum", "Distance", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode")							

airport_dep = df_airline.groupby("Origin").agg(count("Year").alias("origin_cnt")).sort(desc("origin_cnt")).limit(10)
airport_arr = df_airline.groupby("Dest").agg(count("Year").alias("dest_cnt")).sort(desc("dest_cnt")).limit(10)
airport_list = [row.Origin for row in airport_dep.collect()]
df_airline = df_airline.filter(df_airline["Origin"].isin(airport_list) & df_airline["Dest"].isin(airport_list))

df_weather = df_weather.select("Year", "Month", "DayOfMonth", "AirportCode", split(df_weather["Weather"],",").alias("WeatherArray"))
df_weather = df_weather.select("Year", "Month", "DayOfMonth", "AirportCode", explode("WeatherArray").alias("Weather"))
df = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]))

df_1 = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"])).filter(df_airline["CRSDepTime"] < 600)
df_2 = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"])).filter((df_airline["CRSDepTime"] < 1200) & (df["CRSDepTime"] >= 600))
df_3 = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"])).filter((df_airline["CRSDepTime"] < 1800) & (df["CRSDepTime"] >= 1200))
df_4 = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"])).filter((df_airline["CRSDepTime"] < 2400) & (df["CRSDepTime"] >= 1800))



df1 = df_1.agg(mean("ElapsedTimeDelay").alias("ETDM"))
df2 = df_2.agg(mean("ElapsedTimeDelay").alias("ETDM"))
df3 = df_3.agg(mean("ElapsedTimeDelay").alias("ETDM"))
df4 = df_4.agg(mean("ElapsedTimeDelay").alias("ETDM"))
df_result= df1.union(df2).union(df3).union(df4)
z.show(df_result)


df1 = df_1.agg(mean("ArrDelay").alias("ArrM"))
df2 = df_2.agg(mean("ArrDelay").alias("ArrM"))
df3 = df_3.agg(mean("ArrDelay").alias("ArrM"))
df4 = df_4.agg(mean("ArrDelay").alias("ArrM"))
df_result= df1.union(df2).union(df3).union(df4)
z.show(df_result)

df1 = df_1.agg(mean("DepDelay").alias("DepM"))
df2 = df_2.agg(mean("DepDelay").alias("DepM"))
df3 = df_3.agg(mean("DepDelay").alias("DepM"))
df4 = df_4.agg(mean("DepDelay").alias("DepM"))
df_result= df1.union(df2).union(df3).union(df4)
z.show(df_result)

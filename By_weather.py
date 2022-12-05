%spark2.pyspark
from pyspark.sql.functions import *
df_weather = spark.read.csv("hdfs:///user/maria_dev/weather_data/weather_data.csv", header=True, inferSchema=True)

df_2008 = spark.read.csv("hdfs:///user/maria_dev/airline_data/2008.csv", header=True, inferSchema=True)
df_2007 = spark.read.csv("hdfs:///user/maria_dev/airline_data/2007.csv", header=True, inferSchema=True)
df_2006 = spark.read.csv("hdfs:///user/maria_dev/airline_data/2006.csv", header=True, inferSchema=True)
df_2005 = spark.read.csv("hdfs:///user/maria_dev/airline_data/2005.csv", header=True, inferSchema=True)
df_2004 = spark.read.csv("hdfs:///user/maria_dev/airline_data/2004.csv", header=True, inferSchema=True)
df_2003 = spark.read.csv("hdfs:///user/maria_dev/airline_data/2003.csv", header=True, inferSchema=True)
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
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]) 
    | (df_airline["Dest"]==df_weather["AirportCode"]))
df = df.groupby("Weather").agg(count("WeatherDelay").alias("Count")).orderBy(desc("Count"))

z.show(df)

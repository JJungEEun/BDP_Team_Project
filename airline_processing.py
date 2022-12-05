from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
	spark = SparkSession.builder.appName("airline_processing").getOrCreate()
	df = spark.read.load("airline_data/2008.csv", format="csv", sep=",", inferSchema="true", header="true")

	df = df.filter(df["Cancelled"]==0)\
			.filter(df["Diverted"]==0)

	df = df.replace(["NA"], ["0"], "CarrierDelay")\
		.replace(["NA"], ["0"], "WeatherDelay")\
		.replace(["NA"], ["0"], "NASDelay")\
		.replace(["NA"], ["0"], "SecurityDelay")\
		.replace(["NA"], ["0"], "LateAircraftDelay")

	df = df.withColumn("ElapsedTime", df.ArrTime - df.DepTime)\
		.withColumn("CRSElapsedTime", df.CRSArrTime - df.CRSDepTime)\
		.withColumn("ElapsedTimeDelay", df.CRSElapsedTime - df.ActualElapsedTime)\
		.drop("DayOfWeek", "FlightNum", "Distance", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode")
	
	airport_dep = df.groupby("Origin").agg(F.count("Year").alias("origin_cnt")).sort(F.desc("origin_cnt")).limit(10)
	airport_arr = df.groupby("Dest").agg(F.count("Year").alias("dest_cnt")).sort(F.desc("dest_cnt")).limit(10)

	airport_list = [row.Origin for row in airport_dep.collect()]
	df = df.filter(df['Origin'].isin(airport_list) & df['Dest'].isin(airport_list))

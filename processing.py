from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id 

if __name__ == "__main__":
	spark = SparkSession.builder.appName("processing").getOrCreate()
	df = spark.read.load("airline_data/2008.csv",format="csv", sep=",", inferSchema="true", header="true")

	df = df.filter(df["Cancelled"]==0)
	df = df.filter(df["Diverted"]==0)

	df = df.replace(["NA"], ["0"], "CarrierDelay")
	df = df.replace(["NA"], ["0"], "WeatherDelay")
	df = df.replace(["NA"], ["0"], "NASDelay")
	df = df.replace(["NA"], ["0"], "SecurityDelay")
	df = df.replace(["NA"], ["0"], "LateAircraftDelay")
	
	df = df.withColumn("ElapsedTime", df.ArrTime - df.DepTime)
	df = df.withColumn("CRSElapsedTime", df.CRSArrTime - df.CRSDepTime)
	df = df.withColumn("ElapsedTimeDelay", df.CRSElapsedTime - df.ActualElapsedTime)

	df = df.drop("UniqueCarrier", "FlightNum", "Distance", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode")
	
	df.show()
	
	airport_dep = df.groupby("Origin").agg(F.count("Year").alias("origin_cnt")).sort(F.desc("origin_cnt")).limit(10).withColumn("rank", monotonically_increasing_id()+1)
	airport_arr = df.groupby("Dest").agg(F.count("Year").alias("dest_cnt")).sort(F.desc("dest_cnt")).limit(10).withColumn("rank", monotonically_increasing_id()+1)
	airport_top10 = airport_dep.join(airport_arr, airport_arr["rank"]==airport_dep["rank"]).drop("origin_cnt", "dest_cnt")
	
	airport_top10.show()

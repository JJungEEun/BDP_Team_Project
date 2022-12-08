%spark2.pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id 

df = spark.read.load("hdfs:///user/maria_dev/project/2008.csv",format="csv", sep=",", inferSchema="true", header="true")

# 다음 주석까지 preprocessing.py 파일의 전처리 코드와 동일
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
airport_dep = df.groupby("Origin").agg(F.count("Year").alias("origin_cnt")).sort(F.desc("origin_cnt")).limit(10)
airport_arr = df.groupby("Dest").agg(F.count("Year").alias("dest_cnt")).sort(F.desc("dest_cnt")).limit(10)
		
airport_list = [row.Origin for row in airport_dep.collect()]
df = df.filter(df['Origin'].isin(airport_list) & df['Dest'].isin(airport_list))

# 도착 공항 별 평균 도착 지연 시간
is_arrdelay = df.filter("ArrDelay > 0") # 도착 지연이 발생한 행만 filter
count_arrdelay = is_arrdelay.groupby("Dest").agg(F.mean("ArrDelay").alias("delarr")).sort("delarr")
z.show(count_arrdelay)

# 출발 공항 별 평균 출발 지연 시간
is_depdelay = df.filter("DepDelay > 0") # 출발 지연이 발생한 행만 filter
count_depdelay = is_depdelay.groupby("Origin").agg(F.mean("DepDelay").alias("deldep")).sort("deldep")
z.show(count_depdelay)

# 딜레이 별 평균 지연 시간
# 지연이 발생하지 않은 날은 제외하기 위해 0을 결측치로 변환
df = df.replace(["0"], ["NA"], "CarrierDelay")
df = df.replace(["0"], ["NA"], "WeatherDelay")
df = df.replace(["0"], ["NA"], "NASDelay")
df = df.replace(["0"], ["NA"], "SecurityDelay")
df = df.replace(["0"], ["NA"], "LateAircraftDelay")

is_delay = df.select(F.mean("CarrierDelay"), F.mean("WeatherDelay"), F.mean("NASDelay"), F.mean("SecurityDelay"), F.mean("LateAircraftDelay"))
z.show(is_delay)

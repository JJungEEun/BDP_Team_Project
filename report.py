%spark2.pyspark

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

##///노선별 평균 지연 시간
df = df_airline
result_df = df.groupBy("Origin","Dest").agg(F.mean("ElapsedTimeDelay").alias("ETDM"))
z.show(result_df)

##//운항 노선 갯수,딜레이 도착지별
result_df = df.groupBy("Dest").agg(F.count("ElapsedTimeDelay").alias("ETDC")).sort("ETDC")
result_df1 = df.groupBy("Dest").agg(F.mean("ElapsedTimeDelay").alias("ETDM")).sort("ETDM")
result_df2 = result_df.join(result_df1,result_df1["Dest"]==result_df["Dest"])
result_df2 = result_df2.sort("ETDC")
z.show(result_df2)



####
# 출발지 날씨 기준 정렬

df = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]))
result_df = df.groupby("Weather").agg(mean("ElapsedTimeDelay").alias("ETDM"))
result_df1 = df.groupby("Weather").agg(count("Weather").alias("Count"))
result_df2 = result_df.join(result_df1,result_df1["Weather"]==result_df["Weather"])
result_df2 = result_df2.orderBy(desc("ETDM"))
z.show(result_df2)

# 도착 날씨 기준 정렬

df = df_airline.join(df_weather, (df_weather["Year"]==df_airline["Year"]) & (df_weather["Month"]==df_airline["Month"]) 
    & (df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Dest"]==df_weather["AirportCode"]))
result_df = df.groupby("Weather").agg(mean("ElapsedTimeDelay").alias("ETDM"))
result_df1 = df.groupby("Weather").agg(count("Weather").alias("Count"))
result_df2 = result_df.join(result_df1,result_df1["Weather"]==result_df["Weather"])
result_df2 = result_df2.orderBy(desc("ETDM"))
z.show(result_df2)




######
#출발 시간대별 딜레이 정도
(df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]))
df1 = df.filter(df["CRSDepTime"] < 600).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df2 = df.filter((df["CRSDepTime"] < 1200) & (df["CRSDepTime"] >= 600)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df3 = df.filter((df["CRSDepTime"] < 1800) & (df["CRSDepTime"] >= 1200)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df4 = df.filter((df["CRSDepTime"] < 2400) & (df["CRSDepTime"] >= 1800)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df_result= df1.union(df2).union(df3).union(df4)



#도착 시간대별 딜레이 정도.
(df_weather["DayOfMonth"]==df_airline["DayOfMonth"])).filter(df_airline["WeatherDelay"] > 0).filter((df_airline["Origin"]==df_weather["AirportCode"]))
df1 = df.filter(df["CRSDepTime"] < 600).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df2 = df.filter((df["CRSDepTime"] < 1200) & (df["CRSDepTime"] >= 600)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df3 = df.filter((df["CRSDepTime"] < 1800) & (df["CRSDepTime"] >= 1200)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df4 = df.filter((df["CRSDepTime"] < 2400) & (df["CRSDepTime"] >= 1800)).agg(mean("ElapsedTimeDelay").alias("ETDM"))
df_result= df1.union(df2).union(df3).union(df4)
z.show(df_result)

from pyspark import SparkContext, SQLContext, Row
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession
from io import StringIO # python3; python2: BytesIO 
import boto3

sc = SparkContext()

spark = SparkSession(sc)

hadoop = sc._jvm.org.apache.hadoop

fs = hadoop.fs.FileSystem

conf = hadoop.conf.Configuration()

path = hadoop.fs.Path('/dataHW')

print("PATH!!")

print(path)


#df = spark.read.format("csv").option("header", "true").load("hdfs://dataHW/*.csv")


#df.show()


log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")

#for f in fs.get(conf).listStatus(path):
#	LOGGER.info("FILES!!!")
#	print ("FILES!!!")
#	print(f.getPath())
#	print(f.getLen())
#	LOGGER.info(f.getPath())
#	LOGGER.info(f.getPath())
#	LOGGER.info(f.getLen())


#	print (f.getPath(), f.getLen())


#df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://ip-172-31-54-61.ec2.internal:8020/dataHW/*.csv")
####GROUP1.2
df = spark.read.format("csv").option("header", "true").load("hdfs://ip-172-31-49-187.ec2.internal:8020/dataHW/On_Time_On_Time_Performance_*.csv")
####GROUP1.3

print("****************************************************PRINTING SCHEMEA ON TIME PERFORMANCE!!****************************************************")
LOGGER.info("PRINTING SCHEMEA!!")
df.printSchema()
LOGGER.info(df.printSchema())
print(sorted(df.columns))
LOGGER.info(sorted(df.columns))
#print(df.show())

print("****************************************************FINISHED PRINTING SCHEMEA ON TIME PERFORMANCE!!****************************************************")



# F41SCHEDULE = spark.read.format("csv").option("header", "true").load("hdfs://ip-172-31-49-187.ec2.internal:8020/dataHW/*_F41SCHEDULE_*.csv")
# ####GROUP1.3

# print("****************************************************PRINTING SCHEMEA F41SCHEDULE!!****************************************************")
# LOGGER.info("PRINTING SCHEMEA!!")
# F41SCHEDULE.printSchema()
# LOGGER.info(F41SCHEDULE.printSchema())
# print(sorted(F41SCHEDULE.columns))
# LOGGER.info(sorted(F41SCHEDULE.columns))
# #print(df.show())

# print("****************************************************FINISHED PRINTING SCHEMEA F41SCHEDULE!!****************************************************")



Origin_and_Destination_Survey = spark.read.format("csv").option("header", "true").load("hdfs://ip-172-31-49-187.ec2.internal:8020/dataHW/Origin_and_Destination_Survey_*.csv")
####GROUP1.3

print("****************************************************PRINTING SCHEMEA Origin_and_Destination_Survey!!****************************************************")
LOGGER.info("PRINTING SCHEMEA!!")
Origin_and_Destination_Survey.printSchema()
LOGGER.info(Origin_and_Destination_Survey.printSchema())
print(sorted(Origin_and_Destination_Survey.columns))
LOGGER.info(sorted(Origin_and_Destination_Survey.columns))
#print(df.show())

print("****************************************************FINISHED PRINTING SCHEMEA Origin_and_Destination_Survey!!****************************************************")





sqlContext = SQLContext(sc)

####
# 1. Setup (10 points): Download the gbook file and write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)

#lines = sc.textFile('gbooks', 1)
#splitString = lines.flatMap(lambda x: cleanString(x).split())


#val dfWithSchema = spark.createDataFrame(rdd).toDF("id", "vals")

#dfWithSchema.show()



#df = lines.toDF(['column', 'value'])


#temp_var = lines.map(lambda k: k.split('\t'))
#people = temp_var.map(lambda p: Row(word=p[0], count1=int(p[1]), count2=int(p[2]), count3=int(p[3])))
#df = sqlContext.createDataFrame(people)
#df3 = sqlContext.createDataFrame(people)
# Spark SQL - DataFrame API


####
# 5. Joining (10 points): The following program construct a new dataframe out of 'df' with a much smaller size.
####

#df2 = df.select("word", "count1").distinct().limit(100)

#df2.show()
df.createOrReplaceTempView('airports')

Origin_and_Destination_Survey.createOrReplaceTempView('totalAirsports')

#df3.createOrReplaceTempView('gbooks3')
# Now we are going to perform a JOIN operation on 'df2'. Do a self-join on 'df2' in lines with the same #'count1' values and see how many lines this JOIN could produce. Answer this question via DataFrame API and #Spark SQL API
# Spark SQL API

#df2.alias("emp1").join(df2.alias("emp2"),col("emp1.count1") == col("emp2.count1"),"inner").count()


#y = df2.alias("emp1").join(df2.alias("emp2"),"inner").filter(col("emp1.count1") == col("emp2.count1")).show()
#df2.where('r.count1 == l.count1').show()

#innerJoin = people.join(df2, on=['count1'], how='inner')

#inner_join.show()
####GROUP1.2 TASK 1
#airports = sqlContext.sql("SELECT COUNT(Carrier) as count,Carrier,ArrDelay FROM airports WHERE Cancelled != 1.00 AND ArrDelay IS NOT NULL GROUP BY Carrier,ArrDelay ORDER BY ArrDelay,COUNT(Carrier) DESC LIMIT 10")
####GROUP1.3 TASK 1
#airports = sqlContext.sql("SELECT COUNT(DayOfWeek) as count,DayOfWeek,ArrDelay FROM airports WHERE Cancelled != 1.00 AND ArrDelay IS NOT NULL GROUP BY DayOfWeek,ArrDelay ORDER BY ArrDelay,COUNT(DayOfWeek) DESC")
####GROUP2.1 TASK 1
#airports = sqlContext.sql("SELECT COUNT(Carrier) as count,DepDelay,ORIGIN FROM airports WHERE Cancelled != 1.00 AND DepDelay IS NOT NULL AND ORIGIN IN ('CMI','BWI','MIA','LAX','IAH','SFO') GROUP BY Carrier,DepDelay,ORIGIN ORDER BY DepDelay ASC LIMIT 10")
####GROUP2.2 TASK 1
#airports = sqlContext.sql("SELECT COUNT(Dest) as count,DepDelay,ORIGIN FROM airports WHERE Cancelled != 1.00 AND DepDelay IS NOT NULL AND ORIGIN IN ('CMI','BWI','MIA','LAX','IAH','SFO') GROUP BY Dest,DepDelay,ORIGIN ORDER BY DepDelay ASC LIMIT 10")
####GROUP2.3 TASK 1
#airports = sqlContext.sql("WITH CTE_L AS (SELECT Carrier,DepDelay FROM airports WHERE Origin='CMI' AND Dest='ORD' AND Cancelled != 1.00 AND DepDelay IS NOT NULL GROUP BY Carrier,DepDelay UNION ALL SELECT Carrier,DepDelay FROM airports WHERE Origin='IND' AND Dest='CMH' AND Cancelled != 1.00 AND DepDelay IS NOT NULL GROUP BY Carrier,DepDelay UNION ALL SELECT Carrier,DepDelay FROM airports WHERE Origin='DFW' AND Dest='IAH' AND Cancelled != 1.00 AND DepDelay IS NOT NULL GROUP BY Carrier,DepDelay UNION ALL SELECT Carrier,DepDelay FROM airports WHERE Origin='LAX' AND Dest='SFO' AND Cancelled != 1.00 AND DepDelay IS NOT NULL GROUP BY Carrier,DepDelay UNION ALL SELECT Carrier,DepDelay FROM airports WHERE Origin='JFK' AND Dest='LAX' AND Cancelled != 1.00 AND DepDelay IS NOT NULL GROUP BY Carrier,DepDelay UNION ALL SELECT Carrier,DepDelay FROM airports WHERE Origin='ATL' AND Dest='PHX' AND Cancelled != 1.00 AND DepDelay IS NOT NULL GROUP BY Carrier,DepDelay) SELECT * FROM CTE_L  ORDER BY CTE_L.DepDelay LIMIT 10")
###CMILAX
#airports = sqlContext.sql("WITH CTE_L AS (SELECT CMI.Carrier AS Carrier,CMI.DepDelay AS DepDelay,CMI.FlightDate AS FlightDate,CMI.DayOfWeek AS DayOfWeek,CMI.DayofMonth AS DayofMonth,CMI.Month AS Month,CMI.Year AS Year,CMI.Dest as STOP_OVER,CMI.Origin AS Origin,ORD.Dest AS ORIGIN FROM airports AS ORD JOIN (SELECT Carrier,DepDelay,FlightDate,DayOfWeek,DayofMonth,Month,Year,Dest,Origin FROM airports WHERE Origin='CMI' AND Cancelled != 1.00 AND DepDelay IS NOT NULL AND FlightDate = '2008-04-03') AS CMI ON CMI.Dest = ORD.Origin WHERE ORD.Origin='ORD' AND ORD.Dest='LAX' AND ORD.Cancelled != 1.00 AND ORD.DepDelay IS NOT NULL AND ORD.FlightDate = '2008-04-03') SELECT * FROM CTE_L")

#airports = sqlContext.sql("WITH CTE_L AS (SELECT JAX.Carrier,JAX.DepDelay,JAX.FlightDate,JAX.DayOfWeek,JAX.DayofMonth,JAX.Month,JAX.Year,JAX.Dest as STOP_OVER,JAX.Origin,DFW.Dest as FINAL_DEST FROM airports AS DFW JOIN (SELECT Carrier,DepDelay,FlightDate,DayOfWeek,DayofMonth,Month,Year,Dest,Origin FROM airports WHERE Origin='JAX' AND Cancelled != 1.00 AND DepDelay IS NOT NULL AND FlightDate = '2008-09-09') AS JAX ON JAX.Dest = DFW.Origin WHERE DFW.Origin='DFW' AND DFW.Dest='CRP' AND DFW.Cancelled != 1.00 AND DFW.DepDelay IS NOT NULL AND DFW.FlightDate = '2008-09-09') SELECT * FROM CTE_L")
#airports = sqlContext.sql("WITH CTE_L AS (SELECT SLC.Carrier,SLC.DepDelay,SLC.FlightDate,SLC.DayOfWeek,SLC.DayofMonth,SLC.Month,SLC.Year,SLC.Dest as STOP_OVER,SLC.Origin,BFL.Dest as FINAL_DEST FROM airports AS BFL JOIN (SELECT Carrier,DepDelay,FlightDate,DayOfWeek,DayofMonth,Month,Year,Dest,Origin FROM airports WHERE Origin='SLC' AND Cancelled != 1.00 AND DepDelay IS NOT NULL AND FlightDate = '2008-01-04') AS SLC ON SLC.Dest = BFL.Origin WHERE BFL.Origin='BFL' AND BFL.Dest='LAX' AND BFL.Cancelled != 1.00 AND BFL.DepDelay IS NOT NULL AND BFL.FlightDate = '2008-01-04') SELECT * FROM CTE_L")
#airports = sqlContext.sql("WITH CTE_L AS (SELECT LAX.Carrier,LAX.DepDelay,LAX.FlightDate,LAX.DayOfWeek,LAX.DayofMonth,LAX.Month,LAX.Year,LAX.Dest as STOP_OVER,LAX.Origin,SFO.Dest as FINAL_DEST FROM airports AS SFO JOIN (SELECT Carrier,DepDelay,FlightDate,DayOfWeek,DayofMonth,Month,Year,Dest,Origin FROM airports WHERE Origin='LAX' AND Cancelled != 1.00 AND DepDelay IS NOT NULL AND FlightDate = '2008-12-08') AS LAX ON LAX.Dest = SFO.Origin WHERE SFO.Origin='SFO' AND SFO.Dest='PHX' AND SFO.Cancelled != 1.00 AND SFO.DepDelay IS NOT NULL AND SFO.FlightDate = '2008-12-08') SELECT * FROM CTE_L")
#t#eenagers = sqlContext.sql("WITH CTE_L AS (SELECT DFW.Carrier,DFW.DepDelay,DFW.FlightDate,DFW.DayOfWeek,DFW.DayofMonth,DFW.Month,DFW.Year,DFW.Dest as STOP_OVER,DFW.Origin,ORD.Dest as FINAL_DEST FROM airports AS ORD JOIN (SELECT Carrier,DepDelay,FlightDate,DayOfWeek,DayofMonth,Month,Year,Dest,Origin FROM airports WHERE Origin='DFW' AND Cancelled != 1.00 AND DepDelay IS NOT NULL AND FlightDate = '2008-10-06') AS DFW ON DFW.Dest = ORD.Origin WHERE ORD.Origin='ORD' AND ORD.Dest='DFW' AND ORD.Cancelled != 1.00 AND ORD.DepDelay IS NOT NULL AND ORD.FlightDate = '2008-10-06') SELECT * FROM CTE_L")
#airports = sqlContext.sql("WITH CTE_L AS (SELECT LAX.Carrier,LAX.DepDelay,LAX.FlightDate,LAX.DayOfWeek,LAX.DayofMonth,LAX.Month,LAX.Year,LAX.Dest as STOP_OVER,LAX.Origin,ORD.Dest as FINAL_DEST FROM airports AS ORD JOIN (SELECT Carrier,DepDelay,FlightDate,DayOfWeek,DayofMonth,Month,Year,Dest,Origin FROM airports WHERE Origin='LAX' AND Cancelled != 1.00 AND DepDelay IS NOT NULL AND FlightDate = '2008-01-01') AS LAX ON LAX.Dest = ORD.Origin WHERE ORD.Origin='ORD' AND ORD.Dest='JFK' AND ORD.Cancelled != 1.00 AND ORD.DepDelay IS NOT NULL AND ORD.FlightDate = '2008-01-01') SELECT * FROM CTE_L")

airports = sqlContext.sql("SELECT COUNT(Dest) as DEST_COUNT,Dest from airports GROUP BY Dest")

t2 = sqlContext.sql("SELECT COUNT(Dest) as DEST_COUNT,Dest from totalAirsports GROUP BY Dest")

#airports = sqlContext.sql("DESCRIBE airports")
airports.show(truncate = False)

#airports = sqlContext.sql("SELECT * FROM gbooks2")
#selfDf = df2.alias("g").join(df2.alias('r'), on='count1').where('g.count1 == r.count1')
#airports.write.format('csv').save('hdfs://ip-172-31-49-187.ec2.internal:8020/dataHW/Homeww1.3-1.csv')
airports.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save('hdfs://ip-172-31-49-187.ec2.internal:8020/dataHW/HWtotalairports1.csv')

t2.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save('hdfs://ip-172-31-49-187.ec2.internal:8020/dataHW/HWtotalairports2.csv')
#df_load = sparkSession.read.csv('hdfs://cluster/user/hdfs/test/example.csv')

df_load = spark.read.format("csv").option("header", "true").load("hdfs://ip-172-31-49-187.ec2.internal:8020/dataHW/Homeww3.2LAXORD.csv")

df_load.show()
#airports.write.format("csv").save("hdfs://ip-172-31-49-187.ec2.internal:8020/dataHW/1.2-1.csv")
#selfDf.show() 

# output: 210





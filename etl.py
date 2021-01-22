from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, when
sc = SparkContext('local')
spark = SparkSession(sc)
df=spark.read.csv("s3://project-24/temp/2018.csv",header=True)

#d2=d1.fillna(0,subset=['cancellation_code'])
d2=df.withColumn("CANCELLATION_CODE", when(df.CANCELLATION_CODE !="", df.CANCELLATION_CODE).otherwise('F'))
#d2=df
d2=d2.withColumn('Status', when(d2.ARR_DELAY >=1, 1).otherwise(0))
#dropping unnecessary columns from dataframe 
columns_to_drop = ['op_carrier_fl_num', 'dep_time','dep_delay','crs_arr_time','arr_time','cancelled','diverted','crs_elapsed_time','actual_elapsed_time','distance','late_aircraft_delay','Unnamed: 27']
d2=d2.drop(*columns_to_drop)

#change carrier names
d2 = d2.withColumn("OP_CARRIER", when(col("OP_CARRIER") == "UA","United Airlines").when(col("op_carrier") =="AS","Alaska Airlines").when(col("op_carrier") =="9E","Endeavor Air").when(col("op_carrier") =="B6","JetBlue Airways").when(col("op_carrier") =="EV","ExpressJet").when(col("op_carrier") =="F9","Frontier Airlines").when(col("op_carrier") =="G4","Allegiant Air").when(col("op_carrier") =="HA","Hawaiian Airlines").when(col("op_carrier") =="MQ","Envoy Air").when(col("op_carrier") =="NK","Spirit Airlines").when(col("op_carrier") =="OH","PSA Airlines").when(col("op_carrier") =="OO","SkyWest Airlines").when(col("op_carrier") =="VX","Virgin America").when(col("op_carrier") =="WN","Southwest Airlines").when(col("op_carrier") =="YV","Mesa Airline").when(col("op_carrier") =="YX","Republic Airways").when(col("op_carrier") =="AA","American Airlines").when(col("op_carrier") =="DL","Delta Airlines") .otherwise("") )

#date conversion
d2=d2.withColumn("FL_DATE", f.from_unixtime(f.unix_timestamp(d2.FL_DATE), "yyyy-MM-dd"))
df_split = f.split(d2['fl_date'], '-')
d2 = d2.withColumn('YEAR', df_split.getItem(0))
d2 = d2.withColumn('MONTH', df_split.getItem(1))
d2 = d2.withColumn('DAY', df_split.getItem(2))

#fill zero
d2=d2.na.fill(0)
d2.write.parquet("s3://project-24/clean",,mode="overwrite")


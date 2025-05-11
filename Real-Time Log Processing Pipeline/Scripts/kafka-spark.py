#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
# Set the exact package versions for Spark 3.0.1
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.apache.kafka:kafka-clients:2.4.1 pyspark-shell'

# Then create your SparkSession
from pyspark.sql import SparkSession

spark = SparkSession     .builder     .appName("ApacheLogAnalysis")     .config("spark.streaming.stopGracefullyOnShutdown", "true")     .getOrCreate()


# In[2]:


# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, window, count, current_timestamp
from pyspark.sql.types import StringType, IntegerType

# Initialize Spark Session 
spark = SparkSession     .builder     .appName("ApacheLogAnalysis60SecIntervals")     .config("spark.streaming.stopGracefullyOnShutdown", "true")     .getOrCreate()

# Set log level to reduce notebook output noise
spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
kafka_df = spark     .readStream     .format("kafka")     .option("kafka.bootstrap.servers", "localhost:9092")     .option("subscribe", "pykafka")     .option("startingOffsets", "latest")     .load()


# In[7]:


# Parse Kafka value
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as log_line")

logs_df = parsed_df 	.withColumn("ip", regexp_extract(col("log_line"), r'^([\d.]+)', 1)) 	.withColumn("timestamp", regexp_extract(col("log_line"), r'\[(.*?)\]', 1)) 	.withColumn("method", regexp_extract(col("log_line"), r'"(\w+) ', 1)) 	.withColumn("url", regexp_extract(col("log_line"), r'"(?:\w+) (\S+)(?: HTTP/\d\.\d)?', 1)) 	.withColumn("status", regexp_extract(col("log_line"), r' (\d{3}) ', 1).cast("integer")) 	.withColumn("size", regexp_extract(col("log_line"), r' (\d+) ', 1).cast("integer")) 	.withColumn("user_agent", regexp_extract(col("log_line"), r'"([^"]+)"$', 1))



final_df = logs_df.select(
	"ip",
	"timestamp",
	"method",
	"url",
	"status",
	"size",
	"user_agent"
)


# In[ ]:


# Output to console
query = final_df.writeStream     .outputMode("append")     .format("console")     .option("truncate", False)     .start()

query.awaitTermination()


# In[ ]:





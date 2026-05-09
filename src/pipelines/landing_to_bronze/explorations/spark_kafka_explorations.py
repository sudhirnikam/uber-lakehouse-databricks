# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Event Hubs configuration
EH_NAMESPACE  = "uber-app-events"
EH_NAME = "uber-app-events-topic"


EH_CONN_STR  = ""

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest'
}


df = spark.readStream.format("kafka")\
            .options(**KAFKA_OPTIONS)\
            .load()


df = df.withColumn("rides", col("value").cast(StringType()))

display(df, checkpointLocation = "/Volumes/uber_catalog/bronze/lakehouse_bronze_volume/checkpoints" )


# COMMAND ----------

spark.sql("select * from uber_catalog.bronze.rides_raw").show(10, False)
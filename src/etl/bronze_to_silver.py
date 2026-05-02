from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


rides_schema = StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])

select_fields_list = ["ride_id","confirmation_number","passenger_id","driver_id","vehicle_id","pickup_location_id","dropoff_location_id","vehicle_type_id","vehicle_make_id","payment_method_id","ride_status_id","pickup_city_id","dropoff_city_id","cancellation_reason_id","passenger_name","passenger_email","passenger_phone","driver_name","driver_rating","driver_phone","driver_license","vehicle_model","vehicle_color","license_plate","pickup_address","pickup_latitude","pickup_longitude","dropoff_address","dropoff_latitude","dropoff_longitude","distance_miles","duration_minutes","booking_timestamp","pickup_timestamp","dropoff_timestamp","base_fare","distance_fare","time_fare","surge_multiplier","subtotal","tip_amount","total_fare","rating"]

# Empty Streaming Table in silver schema
dp.create_streaming_table(
    name="uber_catalog.silver.stg_rides",
    comment="Staging table for rides data from both bulk and streaming sources"
)

# Bulk/Initial Load
@dp.append_flow(
  target = "uber_catalog.silver.stg_rides"
  )
def rides_bulk():
    df = spark.readStream.table("uber_catalog.bronze.bulk_rides")
    df = df.withColumn("booking_timestamp", col("booking_timestamp").cast("timestamp"))
    df = df.withColumn("rating", col("rating").cast("long"))
    df = df.select(*select_fields_list)
    return df 

# Streaming Load
@dp.append_flow(
  target = "uber_catalog.silver.stg_rides"
  ) 
def rides_stream():
    df = spark.readStream.table("uber_catalog.bronze.rides_raw")
    df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))\
                .select("parsed_rides.*")
    df_parsed = df_parsed.select(*select_fields_list)
    df_parsed = df_parsed.withColumn("rating", col("rating").cast("long"))
    return df_parsed





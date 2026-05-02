# from pyspark.sql import functions as F
# from pyspark import pipelines as dp

# # Base path for all raw files
# RAW_LOCATION = "abfss://raw@dluberlakehousedev.dfs.core.windows.net/ingestion/"
# BASE_SCHEMA = "abfss://raw@dluberlakehousedev.dfs.core.windows.net/schema/"

# # Ingest bulk_rides.json
# @dp.table(
#     name="bulk_rides",
#     comment="Raw bulk rides data ingested from JSON files"
# )
# def bulk_rides():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("pathGlobFilter", "bulk_rides.json")
#         .option("cloudFiles.schemaLocation", BASE_SCHEMA + "bulk_rides")
#         .option("multiLine", "true")
#         .load(RAW_LOCATION)
#     )

# # Ingest map_cancellation_reasons.json
# @dp.table(
#     name="map_cancellation_reasons",
#     comment="Mapping table for cancellation reasons"
# )
# def map_cancellation_reasons():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("pathGlobFilter", "map_cancellation_reasons.json")
#         .option("cloudFiles.schemaLocation", BASE_SCHEMA + "map_cancellation_reasons")
#         .option("multiLine", "true")
#         .load(RAW_LOCATION)
#     )

# # Ingest map_cities.json
# @dp.table(
#     name="map_cities",
#     comment="Mapping table for cities"
# )
# def map_cities():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("pathGlobFilter", "map_cities.json")
#         .option("cloudFiles.schemaLocation", BASE_SCHEMA + "map_cities")
#         .option("multiLine", "true")
#         .load(RAW_LOCATION)
#     )

# # Ingest map_payment_methods.json
# @dp.table(
#     name="map_payment_methods",
#     comment="Mapping table for payment methods"
# )
# def map_payment_methods():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("pathGlobFilter", "map_payment_methods.json")
#         .option("cloudFiles.schemaLocation", BASE_SCHEMA + "map_payment_methods")
#         .option("multiLine", "true")
#         .load(RAW_LOCATION)
#     )

# # Ingest map_ride_statuses.json
# @dp.table(
#     name="map_ride_statuses",
#     comment="Mapping table for ride statuses"
# )
# def map_ride_statuses():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("pathGlobFilter", "map_ride_statuses.json")
#         .option("cloudFiles.schemaLocation", BASE_SCHEMA + "map_ride_statuses")
#         .option("multiLine", "true")
#         .load(RAW_LOCATION)
#     )

# # Ingest map_vehicle_makes.json
# @dp.table(
#     name="map_vehicle_makes",
#     comment="Mapping table for vehicle makes"
# )
# def map_vehicle_makes():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("pathGlobFilter", "map_vehicle_makes.json")
#         .option("cloudFiles.schemaLocation", BASE_SCHEMA + "map_vehicle_makes")
#         .option("multiLine", "true")
#         .load(RAW_LOCATION)
#     )

# # Ingest map_vehicle_types.json
# @dp.table(
#     name="map_vehicle_types",
#     comment="Mapping table for vehicle types"
# )
# def map_vehicle_types():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("pathGlobFilter", "map_vehicle_types.json")
#         .option("cloudFiles.schemaLocation", BASE_SCHEMA + "map_vehicle_types")
#         .option("multiLine", "true")
#         .load(RAW_LOCATION)
#     )

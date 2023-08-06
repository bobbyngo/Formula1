# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file

# COMMAND ----------

#Read json file
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("constructorId", IntegerType(), False),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True),
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json("/mnt/bobbyformula1dl/raw/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

results_df = results_df.withColumnRenamed("resultId", "result_id")\
                        .withColumnRenamed("raceId", "race_id")\
                        .withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("constructorId", "constructor_id")\
                        .withColumnRenamed("positionText", "position_text")\
                        .withColumnRenamed("positionOrder", "position_order")\
                        .withColumnRenamed("fastestLap", "fastest_lap")\
                        .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                        .withColumnRenamed("fastestSpeed", "fastest_speed")\
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_selected_df = results_df.drop(col("statusId"))

# COMMAND ----------

display(results_selected_df)

# COMMAND ----------

results_selected_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/bobbyformula1dl/processed/results")

# COMMAND ----------


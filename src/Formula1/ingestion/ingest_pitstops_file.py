# Databricks notebook source
#Read json file
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

pitstops_df = spark.read.schema(pitstops_schema)\
.option("multiline", True)\
.json("/mnt/bobbyformula1dl/raw/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
pitstops_df.withColumnRenamed("raceId", "race_id")\
           .withColumnRenamed("driverId", "driver_id")\
           .withColumn("igestion_date", current_timestamp())

# COMMAND ----------

pitstops_df.write.mode("overwrite").parquet("/mnt/bobbyformula1dl/processed/pit_stops/")

# COMMAND ----------

display(spark.read.parquet("/mnt/bobbyformula1dl/processed/pit_stops/"))

# COMMAND ----------


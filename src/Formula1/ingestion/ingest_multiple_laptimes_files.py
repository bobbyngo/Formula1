# Databricks notebook source
# Ingest multiple laptimes csv files

# COMMAND ----------

#Read csv file
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

laptimes_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", StringType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

laptimes_df = spark.read.schema(laptimes_schema)\
.csv("/mnt/bobbyformula1dl/raw/lap_times/lap_times_split*.csv")
# or "/mnt/bobbyformula1dl/raw/lap_times/"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
laptimes_df.withColumnRenamed("raceId", "race_id")\
           .withColumnRenamed("driverId", "driver_id")\
           .withColumn("igestion_date", current_timestamp())

# COMMAND ----------

laptimes_df.write.mode("overwrite").parquet("/mnt/bobbyformula1dl/processed/lap_times/")

# COMMAND ----------

display(spark.read.parquet("/mnt/bobbyformula1dl/processed/lap_times/"))

# COMMAND ----------


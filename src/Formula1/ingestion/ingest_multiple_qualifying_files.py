# Databricks notebook source
# Ingest multiple qualifying json files

# COMMAND ----------

#Read json file
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema)\
.option("multiLine", True)\
.json("/mnt/bobbyformula1dl/raw/qualifying/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
           .withColumnRenamed("raceId", "race_id")\
           .withColumnRenamed("driverId", "driver_id")\
           .withColumnRenamed("constructorId", "constructor_id")\
           .withColumn("igestion_date", current_timestamp())

# COMMAND ----------

qualifying_df.write.mode("overwrite").parquet("/mnt/bobbyformula1dl/processed/qualifying/")

# COMMAND ----------

display(spark.read.parquet("/mnt/bobbyformula1dl/processed/qualifying/"))

# COMMAND ----------


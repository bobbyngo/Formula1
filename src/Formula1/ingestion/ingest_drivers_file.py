# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json

# COMMAND ----------

#Read json file
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema, True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json("/mnt/bobbyformula1dl/raw/drivers.json")
drivers_df.printSchema()

# COMMAND ----------

#Rename and concat nested column
from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("driverRef", "driver_ref")\
                        .withColumn("ingestion_date", current_timestamp())\
                        .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df = drivers_df.drop(col("url"))

# COMMAND ----------

drivers_df.write.mode("overwrite").parquet("/mnt/bobbyformula1dl/processed/drivers")

# COMMAND ----------


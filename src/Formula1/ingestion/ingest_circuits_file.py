# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read csv file and apply schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType( fields=[StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True),

])

# COMMAND ----------

circuits_df = spark.read\
    .option("header", True)\
    .schema(circuits_schema)\
    .csv("dbfs:/mnt/bobbyformula1dl/raw/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Select column and changing column name

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
    .withColumnRenamed("circuitRef", "circuit_ref")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("lng", "longitude")\
    .withColumnRenamed("alt", "altitude")
display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write df to data lake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/bobbyformula1dl/processed/circuits")

# COMMAND ----------

display(spark.read.parquet("/mnt/bobbyformula1dl/processed/circuits"))

# COMMAND ----------


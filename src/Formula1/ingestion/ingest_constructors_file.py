# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructor.json

# COMMAND ----------

#Read json file
constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json("/mnt/bobbyformula1dl/raw/constructors.json")
display(constructor_df)

# COMMAND ----------

# drop column
from pyspark.sql.functions import col, current_timestamp

constructor_df=constructor_df.drop(col('url'))

# COMMAND ----------

constructor_final_df = constructor_df.withColumnRenamed("constructorId", "constructor_id")\
                                     .withColumnRenamed("constructorRef", "constructor_ref")\
                                     .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/bobbyformula1dl/processed/constructors/")

# COMMAND ----------

display(spark.read.parquet("/mnt/bobbyformula1dl/processed/constructors"))
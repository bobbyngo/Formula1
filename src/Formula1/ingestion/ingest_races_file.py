# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Races csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType

# COMMAND ----------

display(spark.read.option("header", True).csv("dbfs:/mnt/bobbyformula1dl/raw/races.csv"))

# COMMAND ----------

schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                             StructField("year", IntegerType(), True),
                             StructField("round", IntegerType(), True),
                             StructField("circuitId", IntegerType(), True),
                             StructField("name", StringType(), True),
                             StructField("date", DateType(), True),
                             StructField("time", StringType(), True),
                             StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(schema).csv("dbfs:/mnt/bobbyformula1dl/raw/races.csv")
races_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, concat, lit, current_timestamp

# COMMAND ----------

races_df_timestamp = races_df.withColumn("ingestion_date", current_timestamp())\
                           .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_selected_df = races_df_timestamp.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/bobbyformula1dl/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/bobbyformula1dl/processed/races"))

# COMMAND ----------


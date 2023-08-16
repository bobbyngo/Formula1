# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Races csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType

# COMMAND ----------

display(spark.read.option("header", True).csv(f"{raw_folder_path}/{v_file_date}/races.csv"))

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

races_df = spark.read.option("header", True).schema(schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")
races_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, concat, lit, current_timestamp

# COMMAND ----------

races_df_timestamp = races_df.withColumn("ingestion_date", current_timestamp())\
                           .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                           .withColumn("data_source", lit(v_data_source))\
                           .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_selected_df = races_df_timestamp.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'), col("data_source"), col("file_date"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

#races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")
races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")
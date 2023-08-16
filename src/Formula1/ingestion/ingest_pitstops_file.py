# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Pitstops json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

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
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

pit_stops_with_ingestion_date_df = add_ingestion_date(pitstops_df)

# COMMAND ----------

from pyspark.sql.functions import lit
final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))
           

# COMMAND ----------

#pitstops_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops/")
#pitstops_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;
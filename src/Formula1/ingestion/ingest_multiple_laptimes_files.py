# Databricks notebook source
# Ingest multiple laptimes csv files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")
# or "f"{raw_folder_path}/lap_times/"

# COMMAND ----------

from pyspark.sql.functions import lit
laptimes_df=laptimes_df.withColumnRenamed("raceId", "race_id")\
           .withColumnRenamed("driverId", "driver_id")\
           .withColumn("data_source", lit(v_data_source))
laptimes_df = add_ingestion_date(laptimes_df)

# COMMAND ----------

#laptimes_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times/")
#laptimes_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

#overwrite_partition(laptimes_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta_data(laptimes_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;
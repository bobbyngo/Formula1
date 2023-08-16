# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("constructorId", IntegerType(), False),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True),
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

results_df = results_df.withColumnRenamed("resultId", "result_id")\
                        .withColumnRenamed("raceId", "race_id")\
                        .withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("constructorId", "constructor_id")\
                        .withColumnRenamed("positionText", "position_text")\
                        .withColumnRenamed("positionOrder", "position_order")\
                        .withColumnRenamed("fastestLap", "fastest_lap")\
                        .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                        .withColumnRenamed("fastestSpeed", "fastest_speed")\
                        .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date))
results_df = add_ingestion_date(results_df)                   

# COMMAND ----------

results_selected_df = results_df.drop(col("statusId"))

# COMMAND ----------

# Drop duplicates
results_selected_df = results_selected_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

display(results_selected_df)

# COMMAND ----------

# Method 1 for incremental load 

# COMMAND ----------

# for race_id_list in results_selected_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id ={race_id_list.race_id})")

# COMMAND ----------

# results_selected_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

#Method 2

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.results;

# COMMAND ----------

#overwrite_partition(results_selected_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_selected_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) FROM f1_processed.results
# MAGIC GROUP BY race_id ORDER BY race_id DESC;
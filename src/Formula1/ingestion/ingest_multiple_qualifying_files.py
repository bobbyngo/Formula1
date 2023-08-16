# Databricks notebook source
# Ingest multiple qualifying json files

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
.json(f"{raw_folder_path}/{v_file_date}/qualifying/")

# COMMAND ----------

from pyspark.sql.functions import lit
qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
           .withColumnRenamed("raceId", "race_id")\
           .withColumnRenamed("driverId", "driver_id")\
           .withColumnRenamed("constructorId", "constructor_id")\
           .withColumn("data_source", lit(v_data_source))
qualifying_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

#qualifying_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying/")
#qualifying_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

#overwrite_partition(qualifying_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
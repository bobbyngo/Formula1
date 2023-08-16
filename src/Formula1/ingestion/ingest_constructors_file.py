# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructor.json

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

#Read json file
constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")
display(constructor_df)

# COMMAND ----------

# drop column
from pyspark.sql.functions import col, lit

constructor_df=constructor_df.drop(col('url'))

# COMMAND ----------

constructor_final_df = constructor_df.withColumnRenamed("constructorId", "constructor_id")\
                                     .withColumnRenamed("constructorRef", "constructor_ref")\
                                     .withColumn("data_source", lit(v_data_source))\
                                     .withColumn("file_date", lit(v_file_date))
constructor_final_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors/")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")
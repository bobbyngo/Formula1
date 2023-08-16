# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json

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

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")
drivers_df.printSchema()

# COMMAND ----------

#Rename and concat nested column
from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("driverRef", "driver_ref")\
                        .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                        .withColumn("data_source", lit(v_data_source))\
                        .withColumn("file_date", lit(v_file_date))
drivers_df = add_ingestion_date(drivers_df)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df = drivers_df.drop(col("url"))

# COMMAND ----------

#drivers_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
drivers_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")
# Databricks notebook source
v_result = dbutils.notebook.run("ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_constructors_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_drivers_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_multiple_laptimes_files", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_multiple_qualifying_files", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_pitstops_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_races_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_results_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-04-18"})
v_result

# COMMAND ----------


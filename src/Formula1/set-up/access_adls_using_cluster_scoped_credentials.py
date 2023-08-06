# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

# put fs.azure.account.key.bobbyformula1dl.dfs.core.windows.net 9/IjDXR52CwrwhYQp7Do59Hc1UuBYoxIZjcr2RdX90hiDpA3l3YtpongWUD6Z+MeZwwkqwjUrjm2+AStZSvmTA==
#under spark config advanced options in databricks cluster

# spark.conf.set(
#     "fs.azure.account.key.bobbyformula1dl.dfs.core.windows.net",
#     "9/IjDXR52CwrwhYQp7Do59Hc1UuBYoxIZjcr2RdX90hiDpA3l3YtpongWUD6Z+MeZwwkqwjUrjm2+AStZSvmTA=="
# )

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@bobbyformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@bobbyformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


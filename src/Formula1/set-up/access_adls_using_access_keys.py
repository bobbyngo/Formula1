# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope='bobbyformula1-scope', key='formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.bobbyformula1dl.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@bobbyformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@bobbyformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

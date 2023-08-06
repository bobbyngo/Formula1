# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1_demo_sas_token = dbutils.secrets.get(scope='bobbyformula1-scope', key='formula1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.bobbyformula1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.bobbyformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.bobbyformula1dl.dfs.core.windows.net", formula1_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@bobbyformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@bobbyformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='bobbyformula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='bobbyformula1-scope', key='formula1dl-account-key')

# COMMAND ----------


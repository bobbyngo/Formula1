# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/Client Id, Directory/tenant Id & Secret
# MAGIC 3. Call file system ulity mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope='bobbyformula1-scope', key='formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='bobbyformula1-scope', key='formula1-app-tenant-id')
# Secret value in certificates & secret
client_secret = dbutils.secrets.get(scope='bobbyformula1-scope', key='formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# Mount command
dbutils.fs.mount(
  source = "abfss://demo@bobbyformula1dl.dfs.core.windows.net/",
  mount_point = "/mnt/bobbyformula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bobbyformula1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/bobbyformula1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# Unmount command
# dbutils.fs.unmount('/mnt/bobbyformula1dl/demo')
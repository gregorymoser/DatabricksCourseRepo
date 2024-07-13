# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Keys

# COMMAND ----------

formula1dlaux_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlaux-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure,account.key.formula1dlaux.dfs.core.windows.net",
    formula1dlaux_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dlaux.dfs.core.windows.net")

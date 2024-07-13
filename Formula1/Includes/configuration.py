# Databricks notebook source
# MAGIC %md
# MAGIC #### Variables containing the folder path for Raw, Processed and Presentation containers

# COMMAND ----------

raw_folder_path = '/mnt/formula1dlaux/raw'
processed_folder_path = '/mnt/formula1dlaux/processed'
presentation_folder_path = '/mnt/formula1dlaux/presentation'

# COMMAND ----------

'''
abfs protocol instead of the mount for student account

raw_folder_path = 'abfss://raw@formula1dl.dfs.core.windows.net'
processed_folder_path = 'abfss://processed@formula1dl.dfs.core.windows.net'
presentation_folder_path = 'abfss://presentation@formula1dl.dfs.core.windows.net'
'''

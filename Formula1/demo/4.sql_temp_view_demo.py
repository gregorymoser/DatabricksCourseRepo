# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# race_results_df.createTempView("v_race_results")
race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temporary View

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC -- FROM gv_race_results
# MAGIC FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()

# COMMAND ----------



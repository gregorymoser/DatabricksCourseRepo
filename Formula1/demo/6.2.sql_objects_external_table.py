# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.write.format("parquet"). \
  option("path", f"{presentation_folder_path}/race_results_ext_py"). \
  saveAsTable("demo.race_results_ext_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED demo.race_results_ext_py;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE demo.race_results_ext_sql
# MAGIC (
# MAGIC   race_year INT,
# MAGIC   race_name STRING,
# MAGIC   race_date TIMESTAMP,
# MAGIC   circuit_location STRING,
# MAGIC   driver_name STRING,
# MAGIC   driver_number INT,
# MAGIC   driver_nationality STRING,
# MAGIC   team STRING,
# MAGIC   grid INT,
# MAGIC   fastest_lap INT,
# MAGIC   race_time STRING,
# MAGIC   points FLOAT,
# MAGIC   position INT,
# MAGIC   created_date TIMESTAMP
# MAGIC )
# MAGIC
# MAGIC USING parquet
# MAGIC LOCATION "/mnt/formula1dlaux/presentation/race_results_ext_sql"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO demo.race_results_ext_sql
# MAGIC SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT (1) FROM demo.race_results_ext_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.race_results_ext_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo.race_results_ext_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo;

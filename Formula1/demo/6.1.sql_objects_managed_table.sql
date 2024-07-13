-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(race_results_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DESC race_results_python;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE default.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------



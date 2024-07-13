-- Databricks notebook source
-- CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

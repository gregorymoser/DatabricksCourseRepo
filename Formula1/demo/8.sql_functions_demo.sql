-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SELECT * FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref
FROM drivers;

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')
FROM drivers;

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')[0] forename,  SPLIT(name, ' ')[1] surename
FROM drivers;

-- COMMAND ----------

SELECT *, current_timestamp()
FROM drivers;

-- COMMAND ----------

-- change data format of dob -> function date_format()
SELECT *, date_format(dob, 'dd-MM-yyyy')
FROM drivers;

-- COMMAND ----------

-- adding a day to the dob -> function date_add
SELECT *, date_add(dob, 1)
FROM drivers;

-- COMMAND ----------

SELECT COUNT(*) FROM drivers;

-- COMMAND ----------

SELECT MAX(dob)
FROM drivers;

-- COMMAND ----------

SELECT * 
FROM drivers
WHERE dob = '2000-05-11';

-- COMMAND ----------

SELECT MIN(dob) FROM drivers;

-- COMMAND ----------

SELECT * FROM drivers
WHERE dob = '1896-12-28';

-- COMMAND ----------

SELECT count(*) FROM drivers
WHERE nationality = 'British';

-- COMMAND ----------

SELECT nationality, count(*) FROM drivers
GROUP BY nationality
ORDER BY nationality ASC;

-- COMMAND ----------

SELECT nationality, count(*) 
FROM drivers
GROUP BY nationality
HAVING COUNT(*) > 100
ORDER BY nationality ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Window Functions

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC)
AS age_rank
FROM drivers
ORDER BY nationality, age_rank;

-- COMMAND ----------



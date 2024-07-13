-- Databricks notebook source
-- list all databases
SHOW DATABASES;

-- COMMAND ----------

-- look at all the data that has been ingested
-- To do so, we need to list all the tables in f1_processed database

-- To check the current database
SELECT current_database();

-- COMMAND ----------

-- To choose which database to use
USE f1_processed;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- To see the tables in current database
SHOW TABLES;

-- COMMAND ----------

-- To look at drivers data for instance
-- SELECT * to list all the columns
SELECT * FROM f1_processed.drivers;
-- only drivers would also work coz we have already selected f1_processed database
-- Databricks limits the numer of records to 1000

-- COMMAND ----------

-- To limit the number of records to be shown
SELECT * 
FROM drivers
LIMIT 10;

-- COMMAND ----------

-- describe the table
-- List of column name, data types and comments
DESC drivers;


-- COMMAND ----------

-- apply a condition: find all the drivers whose the nationality is british
SELECT * FROM drivers
WHERE nationality = 'British';

-- COMMAND ----------

-- Drivers whose the nationality is british and born after 1990
SELECT * FROM drivers
WHERE nationality = 'British' AND dob >= '1990-01-01';

-- COMMAND ----------

-- to select a few column names
-- give alias to the column names
SELECT name AS Name, driver_id AS ID, dob AS date_of_birth FROM drivers
WHERE nationality = 'Brazilian';

-- COMMAND ----------

-- to order by a specific column
-- ORDER BY can be used with ASC (default) or DESC
SELECT name AS Name, driver_id AS ID, dob AS date_of_birth FROM drivers
WHERE nationality = 'Brazilian'
ORDER BY Name ASC;

-- COMMAND ----------

-- ORDER BY multiple columns
SELECT * FROM drivers
ORDER BY name ASC, nationality, dob DESC;

-- COMMAND ----------

-- OR condition
SELECT name, nationality, dob AS date_of_birth FROM drivers
WHERE (nationality = 'British' AND dob >= '1990-01-01') OR nationality = 'Brazilian'
ORDER BY dob DESC;

-- COMMAND ----------



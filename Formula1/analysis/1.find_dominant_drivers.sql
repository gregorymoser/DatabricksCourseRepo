-- Databricks notebook source
-- Check sum of points for each driver
SELECT driver_name,
  sum(calculated_points) as total_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
ORDER BY total_points DESC;

-- COMMAND ----------

/*
  Count the number of records for each driver
  will tell us how many races he did
*/

SELECT driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) as total_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
ORDER BY total_points DESC;

-- COMMAND ----------

/*
Average points each driver has scored across races
restrict number of races for at least 50,
otherwise drivers who have raced only once and won, would be on top of the list
*/

SELECT driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

-- Same analysis, but only fro the last decade
SELECT driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 and 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

-- 2001 to 2010
SELECT driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 and 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

-- 21st century
SELECT driver_name,
  COUNT(1) AS total_races,
  SUM(calculated_points) AS total_points,
  AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 and 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------



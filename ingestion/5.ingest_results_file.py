# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

# DBTITLE 1,ime
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False), 
                                    StructField("raceId", IntegerType(), True), 
                                    StructField("driverId", IntegerType(), True), 
                                    StructField("constructorId", IntegerType(), True), 
                                    StructField("number", IntegerType(), True), 
                                    StructField("grid", IntegerType(), True), 
                                    StructField("position", IntegerType(), True), 
                                    StructField("positionText", StringType(), True), 
                                    StructField("positionOrder", IntegerType(), True), 
                                    StructField("points", FloatType(), True), 
                                    StructField("laps", IntegerType(), True), 
                                    StructField("time", StringType(), True), 
                                    StructField("milliseconds", IntegerType(), True), 
                                    StructField("fastestLap", IntegerType(), True), 
                                    StructField("rank", IntegerType(), True), 
                                    StructField("fastestLapTime", StringType(), True), 
                                    StructField("fastestLapSpeed", FloatType(), True), 
                                    StructField("statusId", StringType(), True), 
])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json("/mnt/formula1dlaux/raw/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId","result_id") \
                                .withColumnRenamed("raceId","race_id") \
                                .withColumnRenamed("driverId","driver_id") \
                                .withColumnRenamed("constructorId","constructor_id") \
                                .withColumnRenamed("positionText","position_text") \
                                .withColumnRenamed("positionOrder","position_order") \
                                .withColumnRenamed("fastestLap","fastest_lap") \
                                .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn('data_source', lit(v_data_source))


# COMMAND ----------

display(results_with_columns_df)

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------


 results_final_df.write.mode("append") \
     .partitionBy("race_id") \
     .format("parquet") \#     .saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2
# MAGIC

# COMMAND ----------

results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id",
                                           "number", "grid", "position", "position_text",
                                           "position_order", "points", "laps", "time",
                                           "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlaux/processed/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")

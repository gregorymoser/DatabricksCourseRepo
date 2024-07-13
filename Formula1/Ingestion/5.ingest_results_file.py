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

# results_df = spark.read \
#     .schema(results_schema) \
#     .json("/mnt/formula1dlaux/raw/results.json")

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")


# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

'''
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
'''
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
                                .withColumn('data_source', lit(v_data_source)) \
                                .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df) 

# COMMAND ----------

display(results_with_columns_df)

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#      spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

'''
results_final_df.write.mode("overwrite").partitionBy("race_id") \
    .parquet("/mnt/formula1dlaux/processed/results")

results_final_df.write.mode("overwrite") \
    .partitionBy("race_id") \
    .format("parquet") \
    .saveAsTable("f1_processed.results")
'''
# Incremental load rather then full load
# results_final_df.write.mode("append") \
#     .partitionBy("race_id") \
#     .format("parquet") \
#     .saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results;

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id",
                                           "number", "grid", "position", "position_text",
                                           "position_order", "points", "laps", "time",
                                           "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

# def re_arrange_partition_column(input_df, partition_column):
#     # partition_column = 'race_id'
#     column_list = []
#     for column_name in input_df.schema.names:
#         if column_name != partition_column:
#             column_list.append(column_name)
#     column_list.append(partition_column)
#     # print(column_list)
#     output_df = input_df.select(column_list)
#     return output_df

# COMMAND ----------

# output_df = re_arrange_partition_column(results_final_df,'race_id')

# COMMAND ----------

# def overwrite_partition(input_df, db_name, table_name, partition_column):
#     output_df = re_arrange_partition_column(input_df, partition_column)
#     spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
#     if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#         output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#     else:
#         output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"

# merge_delta_data(results_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"

merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# def merge_delta_data(input_df, db_name, table_name, folder_path):

#     spark.conf.set("spark.databreicks.optimizer.dynamicPartitionPruning","true")

#     from delta.tables import DeltaTable
#     if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#         deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
#         deltaTable.alias("tgt").merge(
#             input_df.alias("src"),
#             "tgt.result_id = src.result_id AND tgt.race_id = src.race_id") \
#             .whenMatchedUpdateAll() \
#             .whenNotMatchedInsertAll() \
#             .execute()
#     else:
#         input_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable(f"{db_name}.{table_name}")

# def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):

#     spark.conf.set("spark.databreicks.optimizer.dynamicPartitionPruning","true")

#     from delta.tables import DeltaTable
#     if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#         deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
#         deltaTable.alias("tgt").merge(
#             input_df.alias("src"),
#             merge_condition) \
#             .whenMatchedUpdateAll() \
#             .whenNotMatchedInsertAll() \
#             .execute()
#     else:
#         input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("append") \
#      .insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode("append") \
#      .partitionBy("race_id") \
#      .format("parquet") \
#      .saveAsTable("f1_processed.results")

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dlaux/processed/results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1)
# MAGIC from f1_processed.results
# MAGIC where file_date = '2021-03-21'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results WHERE race_id = 540 AND driver_id = 229;

# COMMAND ----------

dbutils.notebook.exit("Success")

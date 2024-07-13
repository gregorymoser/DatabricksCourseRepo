# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read the JSON file using the Spark DataFrame Reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/formula1dlaux/raw

# COMMAND ----------

'''
constructor_df = spark.read.schema(constructors_schema).json("dbfs:/mnt/formula1dlaux/raw/constructors.json")
'''
constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Drop unwanted column from the DataFrame (URL)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# constructor_dropped_df = constructor_df.drop(constructor_df['url'])
# constructor_dropped_df = constructor_df.drop(col('url'))
constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# combining two different API calls
'''
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorRef", "constructor_ref") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn('data_source', lit(v_data_source))
'''
constructor_renamed_df = constructor_dropped_df \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_data', lit(v_file_date))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_renamed_df)

# COMMAND ----------

# display(constructor_final_df)

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dlaux/processed/constructors")

# constructor_final_df.write.mode("overwrite") \
#     .format("parquet") \
#     .saveAsTable("f1_processed.constructors")

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# %fs
# ls /mnt/formula1dlaux/processed/constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")

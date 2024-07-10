# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read the JSON file using the Spark DataFrame Reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlaux/raw

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json("dbfs:/mnt/formula1dlaux/raw/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Drop unwanted column from the DataFrame (URL)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorRef", "constructor_ref") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dlaux/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlaux/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("Success")

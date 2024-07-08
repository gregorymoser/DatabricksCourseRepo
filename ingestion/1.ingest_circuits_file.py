# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv File

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Read the CSV File using the Spark DataFrame Reader
# MAGIC 2. Specifying the Schema
# MAGIC 3. Select Columns
# MAGIC 4. WithColumnRenamed

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

print(raw_folder_path)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlaux/raw

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.show(n=10, truncate=True, vertical=False)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only the required columns

# COMMAND ----------

# 1st method
circuits_select_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# 2nd method
circuits_select_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, \
                                         circuits_df.name, circuits_df.location, circuits_df.country, \
                                             circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# 3rd method
circuits_select_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], \
                                         circuits_df["name"], circuits_df["location"], circuits_df["country"], \
                                             circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# 4th method

from pyspark.sql.functions import col

# 1st method
circuits_select_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), 
                                        col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit
# lit converts a text into column type

# COMMAND ----------

circuits_renamed_df = circuits_select_df.withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('circuitRef', 'circuit_ref') \
    .withColumnRenamed('lat', 'latitude') \
    .withColumnRenamed('lng', 'longitude') \
    .withColumnRenamed('alt', 'altitude') \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as Parquet format

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

'''
%fs
ls /mnt/formula1dlaux/processed/circuits
'''

# COMMAND ----------

# temp_df = spark.read.parquet("/mnt/formula1dlaux/processed/circuits")

# COMMAND ----------

# display(temp_df)

# COMMAND ----------

# dbutils.notebook.exit("Success")

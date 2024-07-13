# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# python way1: races_filtered_df = races_df.filter(races_df.race_year = 2019)
# python way2: races_filtered_df = races_df.filter(races_df["race_year"] == 2019)
races_filtered_df = races_df.filter("race_year = 2019") # sql way

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

# specifying multiple conditions
# races_filtered_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["race_round"] <= 5)

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5") # sql way

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------



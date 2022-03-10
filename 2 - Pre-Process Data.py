# Databricks notebook source
# MAGIC %md 
# MAGIC # Pre-Process Data
# MAGIC 
# MAGIC ## Purpose
# MAGIC Do some initial work to unzip the input files and convert them to parquet for increased performance when querying

# COMMAND ----------

# MAGIC %md ## Process Companies House

# COMMAND ----------

# MAGIC %md ### Unzip Data

# COMMAND ----------

# MAGIC %sh unzip /dbfs/mnt/lake/RAW/CompaniesHouse/BasicCompanyData.zip

# COMMAND ----------

# MAGIC %sh cp BasicCompanyDataAsOneFile-2022-03-01.csv /dbfs/mnt/lake/RAW/CompaniesHouse/

# COMMAND ----------

# MAGIC %md ### Read CSV

# COMMAND ----------

df = spark.read.option("header", "true").csv("/mnt/lake/RAW/CompaniesHouse/*.csv")

# COMMAND ----------

for col in df.columns:
    df = df.withColumnRenamed(col, col.replace(" ", "").replace(".", "_"))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md ### Output to BASE as Parquet

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/lake/BASE/CompaniesHouse")

# COMMAND ----------

# MAGIC %md ## Process Land Registry

# COMMAND ----------

# MAGIC %md ### Unzip Data

# COMMAND ----------

# MAGIC %sh unzip /dbfs/mnt/lake/RAW/LandRegistry/CCOD_FULL_2022_03.zip

# COMMAND ----------

# MAGIC %sh cp CCOD_FULL_2022_03.csv /dbfs/mnt/lake/RAW/LandRegistry/

# COMMAND ----------

# MAGIC %md ### Read CSV

# COMMAND ----------

df = spark.read.option("header", "true").csv("/mnt/lake/RAW/LandRegistry/*.csv")

# COMMAND ----------

for col in df.columns:
    df = df.withColumnRenamed(col, col.replace(" ", "").replace(".", "_").replace("(", "").replace(")", ""))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md ### Output to BASE as Parquet

# COMMAND ----------

df.write.mode("overwrite").parquet("/mnt/lake/BASE/LandRegistry")
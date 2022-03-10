# Databricks notebook source
from pyspark.sql.functions import col, lit, desc
from pyspark.sql.types import DecimalType

# COMMAND ----------

landRegistryDf = spark.read.parquet("/mnt/lake/BASE/LandRegistry").alias("LR")
matchResultsDf = spark.read.parquet("/mnt/lake/MATCHED/").filter("MatchStatus != 'Not Matched'").alias("M")

# COMMAND ----------

landRegistryDf.display()

# COMMAND ----------

matchResultsDf.display()

# COMMAND ----------

joinedDf = landRegistryDf.join(matchResultsDf, ["TitleNumber"])

# COMMAND ----------

joinedDf.groupBy("M.MatchedCompanyName").count().sort(desc("count")).limit(10).display()

# COMMAND ----------

joinedDf.filter("District = 'MOLE VALLEY'").groupBy("M.MatchedCompanyName").count().sort(desc("count")).limit(10).display()

# COMMAND ----------

joinedDf.filter("Region = 'SOUTH EAST'").groupBy("M.MatchedCompanyName").count().sort(desc("count")).limit(10).display()

# COMMAND ----------

joinedDf \
    .withColumn("PricePaid", col("PricePaid").cast(DecimalType())) \
    .filter("PricePaid IS NOT NULL AND Region = 'SOUTH EAST'") \
    .groupBy("M.MatchedCompanyName") \
    .sum("PricePaid") \
    .sort(desc("sum(PricePaid)")) \
    .limit(10) \
    .display()
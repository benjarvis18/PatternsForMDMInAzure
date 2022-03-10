# Databricks notebook source
# MAGIC %md 
# MAGIC # Match Data
# MAGIC 
# MAGIC ## Purpose
# MAGIC Execute matching rules to match organisations from Land Registry back to Companies House

# COMMAND ----------

from pyspark.sql.functions import col, lit, desc, expr
from pyspark.sql.types import DoubleType, StringType

# COMMAND ----------

# MAGIC %md ## Read in Companies House and Land Registry Data

# COMMAND ----------

companiesHouseDf = spark.read.parquet("/mnt/lake/STANDARDISED/CompaniesHouse").alias("CH")

# COMMAND ----------

landRegistryDf = spark.read.parquet("/mnt/lake/STANDARDISED/LandRegistry").alias("LR")

# COMMAND ----------

# MAGIC %md ## Matching Rule 1 - Companies House Number

# COMMAND ----------

companyNumberMatchesDf = landRegistryDf \
    .join(companiesHouseDf.filter(col("IsPreviousName") == False), ["CompanyNumber"], how="inner") \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        col("LR.CompanyNumber").alias("MatchedCompanyNumber"),
        col("LR.CompanyName").alias("InputCompanyName"),        
        col("CH.CompanyName").alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        col("CH.Postcode").alias("MatchedPostcode")
    )

# COMMAND ----------

companyNumberMatchesDf.display()

# COMMAND ----------

# MAGIC %md ### Match Percentage

# COMMAND ----------

print("Match % = " + str(round((companyNumberMatchesDf.count() / landRegistryDf.count()) * 100, 2)) + "%")

# COMMAND ----------

# MAGIC %md ### Potential Overmatches

# COMMAND ----------

companyNumberMatchesDf.filter("InputCompanyName != MatchedCompanyName").filter("InputCompanyNumber = '00187552'").display()

# COMMAND ----------

# MAGIC %md ## Matching Rule 2 - Companies House Number and Company Name

# COMMAND ----------

companyNumberAndCompanyNameMatchesDf = landRegistryDf \
    .join(companiesHouseDf.filter(col("IsPreviousName") == False), ["CompanyNumber", "CompanyName"], how="inner") \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        col("LR.CompanyNumber").alias("MatchedCompanyNumber"),
        col("LR.CompanyName").alias("InputCompanyName"),        
        col("LR.CompanyName").alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        col("CH.Postcode").alias("MatchedPostcode")
    )

# COMMAND ----------

companyNumberAndCompanyNameMatchesDf.display()

# COMMAND ----------

# MAGIC %md ### Match Percentage

# COMMAND ----------

print("Match % = " + str(round((companyNumberAndCompanyNameMatchesDf.count() / landRegistryDf.count()) * 100, 2)) + "%")

# COMMAND ----------

# MAGIC %md ## Matching Rule 3 - Companies House Number and Postcode

# COMMAND ----------

companyNumberAndPostcodeMatchesDf = landRegistryDf \
    .join(companiesHouseDf.filter(col("IsPreviousName") == False), ["CompanyNumber", "Postcode"], how="inner") \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        col("LR.CompanyNumber").alias("MatchedCompanyNumber"),
        col("LR.CompanyName").alias("InputCompanyName"),        
        col("CH.CompanyName").alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        col("LR.Postcode").alias("MatchedPostcode")
    )

# COMMAND ----------

companyNumberAndPostcodeMatchesDf.display()

# COMMAND ----------

# MAGIC %md ## Matching Rule 4 - Company Name and Postcode

# COMMAND ----------

companyNameAndPostcodeMatchesDf = landRegistryDf \
    .join(companiesHouseDf, ["CompanyName", "Postcode"], how="inner") \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        col("CH.CompanyNumber").alias("MatchedCompanyNumber"),
        col("LR.CompanyName").alias("InputCompanyName"),        
        col("LR.CompanyName").alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        col("LR.Postcode").alias("MatchedPostcode")
    )

# COMMAND ----------

companyNameAndPostcodeMatchesDf.display()

# COMMAND ----------

# MAGIC %md ## Matching Rule 5 - Company Number, Company Name and Postcode

# COMMAND ----------

companyNumberCompanyNameAndPostcodeMatchesDf = landRegistryDf \
    .join(companiesHouseDf, ["CompanyNumber", "CompanyName", "Postcode"], how="inner") \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        col("LR.CompanyNumber").alias("MatchedCompanyNumber"),
        col("LR.CompanyName").alias("InputCompanyName"),        
        col("LR.CompanyName").alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        col("LR.Postcode").alias("MatchedPostcode")
    )

# COMMAND ----------

companyNumberCompanyNameAndPostcodeMatchesDf.display()

# COMMAND ----------

# MAGIC %md ## Bringing it together
# MAGIC 
# MAGIC We'll keep the matching rules that combine multiple attributes together as they give us the best possibility of a match and reduce the possibility of overmatching

# COMMAND ----------

landRegistryMatchDf = landRegistryDf

# Company Number, Company Name and Postcode
companyNumberCompanyNameAndPostcodeMatchesDf = landRegistryMatchDf \
    .join(companiesHouseDf, ["CompanyNumber", "CompanyName", "Postcode"], how="inner") \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        col("LR.CompanyNumber").alias("MatchedCompanyNumber"),
        col("LR.OriginalCompanyName").alias("InputCompanyName"),        
        col("LR.OriginalCompanyName").alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        col("LR.Postcode").alias("MatchedPostcode"),
        lit("Company Number, Company Name and Postcode").alias("MatchAttributes"),
        lit(1.0).alias("MatchConfidencePercentage"),
        lit("Confirmed").alias("MatchStatus")
    )

landRegistryMatchDf = landRegistryMatchDf.join(companyNumberCompanyNameAndPostcodeMatchesDf, "TitleNumber", how="left_anti")

# Company Number and Company Name
companyNumberAndCompanyNameMatchesDf = landRegistryMatchDf \
    .join(companiesHouseDf.filter(col("IsPreviousName") == False), ["CompanyNumber", "CompanyName"], how="inner") \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        col("LR.CompanyNumber").alias("MatchedCompanyNumber"),
        col("LR.OriginalCompanyName").alias("InputCompanyName"),        
        col("LR.OriginalCompanyName").alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        col("CH.Postcode").alias("MatchedPostcode"),
        lit("Company Number and Company Name").alias("MatchAttributes"),
        lit(0.99).alias("MatchConfidencePercentage"),
        lit("Confirmed").alias("MatchStatus")
    )

landRegistryMatchDf = landRegistryMatchDf.join(companyNumberAndCompanyNameMatchesDf, "TitleNumber", how="left_anti")

# Company Name and Postcode
companyNameAndPostcodeMatchesDf = landRegistryMatchDf \
    .join(companiesHouseDf, ["CompanyName", "Postcode"], how="inner") \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        col("CH.CompanyNumber").alias("MatchedCompanyNumber"),
        col("LR.OriginalCompanyName").alias("InputCompanyName"),        
        col("LR.OriginalCompanyName").alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        col("LR.Postcode").alias("MatchedPostcode"),
        lit("Company Name and Postcode").alias("MatchAttributes"),
        lit(0.97).alias("MatchConfidencePercentage"),
        lit("Confirmed").alias("MatchStatus")
    )

landRegistryMatchDf = landRegistryMatchDf.join(companyNameAndPostcodeMatchesDf, "TitleNumber", how="left_anti")

# Company Number and Postcode
companyNumberAndPostcodeMatchesDf = landRegistryMatchDf \
    .join(companiesHouseDf.filter(col("IsPreviousName") == False), ["CompanyNumber", "Postcode"], how="inner") \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        col("LR.CompanyNumber").alias("MatchedCompanyNumber"),
        col("LR.OriginalCompanyName").alias("InputCompanyName"),        
        col("CH.OriginalCompanyName").alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        col("LR.Postcode").alias("MatchedPostcode"),
        lit("Company Number and Postcode").alias("MatchAttributes"),
        lit(0.98).alias("MatchConfidencePercentage"),
        lit("Unconfirmed").alias("MatchStatus")
    )

landRegistryMatchDf = landRegistryMatchDf.join(companyNumberAndPostcodeMatchesDf, "TitleNumber", how="left_anti")

# Mark everything else as not matched
noMatchDf = landRegistryMatchDf \
    .select(
        col("LR.TitleNumber"),
        col("LR.CompanyNumber").alias("InputCompanyNumber"),
        lit(None).alias("MatchedCompanyNumber"),
        col("LR.OriginalCompanyName").alias("InputCompanyName"),        
        lit(None).alias("MatchedCompanyName"),        
        col("LR.Postcode").alias("InputPostcode"),            
        lit(None).alias("MatchedPostcode"),
        lit("None").alias("MatchAttributes"),
        lit(None).alias("MatchConfidencePercentage"),
        lit("Not Matched").alias("MatchStatus")
    )

landRegistryMatchDf = landRegistryMatchDf.join(noMatchDf, "TitleNumber", how="left_anti")

# COMMAND ----------

# Check there's no records left
landRegistryMatchDf.count()

# COMMAND ----------

matchResultDf = companyNumberCompanyNameAndPostcodeMatchesDf \
    .union(companyNumberAndCompanyNameMatchesDf) \
    .union(companyNumberAndPostcodeMatchesDf) \
    .union(companyNameAndPostcodeMatchesDf) \
    .union(noMatchDf)

# COMMAND ----------

matchResultDf.write.mode("overwrite").parquet("/mnt/lake/MATCHED/")

# COMMAND ----------

# MAGIC %md ## Summarise the results

# COMMAND ----------

matchResultDf.groupBy("MatchAttributes", "MatchConfidencePercentage", "MatchStatus").count().sort(desc("MatchConfidencePercentage")).display()

# COMMAND ----------

# MAGIC %md ## Review the remaining records

# COMMAND ----------

notMatchedDf = landRegistryDf \
    .join(matchResultDf, ["TitleNumber"]) \
    .filter(
        col("MatchAttributes") == "None"
    )

# COMMAND ----------

notMatchedDf.display()

# COMMAND ----------

# MAGIC %md ## Leftovers - Fuzzy Matching
# MAGIC 
# MAGIC We can't fuzzy match on company name alone as there is potential for duplication in the companies house dataset. If we combine a direct match on an attribute with a fuzzy match then it allows us to increase our confidence and increase performance by reducing the number of records we fuzzy match.

# COMMAND ----------

# MAGIC %md ### Install Fuzzy Matching Libraries

# COMMAND ----------

spark.udf.registerJavaFunction('jaro_winkler', 'uk.gov.moj.dash.linkage.JaroWinklerSimilarity', DoubleType())                                
                                
spark.udf.registerJavaFunction('jaccard_sim', 'uk.gov.moj.dash.linkage.JaccardSimilarity', DoubleType())                          
                                
spark.udf.registerJavaFunction('cosine_distance', 'uk.gov.moj.dash.linkage.CosineDistance', DoubleType())                 

spark.udf.registerJavaFunction('QgramTokeniser', 'uk.gov.moj.dash.linkage.QgramTokeniser', StringType())                 

# COMMAND ----------

# MAGIC %md ### Run Jaro Winkler against records

# COMMAND ----------

fuzzyMatchDf = notMatchedDf \
    .join(companiesHouseDf, ["CompanyNumber"]) \
    .filter("replace(LR.CompanyName, ' ', '') != replace(CH.CompanyName, ' ', '')") \
    .select(col("LR.OriginalCompanyName").alias("LandRegistry_CompanyName"), col("CH.OriginalCompanyName").alias("CompaniesHouse_CompanyName"), expr("jaro_winkler(LR.CompanyName, CH.CompanyName) AS JaroWinkler")) \
    .filter("JaroWinkler > 0.8") \
    .distinct() \
    .sort(desc("JaroWinkler"))

# COMMAND ----------

fuzzyMatchDf.display()

# COMMAND ----------

# MAGIC %md ## Fuzzy Match Examples

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC LandRegistry = WOLVERHAMPTON CITY AND WALSALL LIFT ACCO**M**ODATION SERVICES LIMITED
# MAGIC 
# MAGIC CompaniesHouse = WOLVERHAMPTON CITY AND WALSALL LIFT ACCO**MM**ODATION SERVICES LIMITED

# COMMAND ----------

fuzzyMatchDf.filter("LandRegistry_CompanyName = 'WOLVERHAMPTON CITY AND WALSALL LIFT ACCOMODATION SERVICES LIMITED'").display()

# COMMAND ----------

# MAGIC %md ## Combining Algorithms

# COMMAND ----------

# MAGIC %md In some cases algorithms may score a record highly even though it isn't technically the same item. This happens most often where there are less letters in each word. To get around this we can use different algorithms to further enhance our match score.

# COMMAND ----------

spark \
    .sql("SELECT 'AGE UK LIMITED' AS SourceName, 'AEG UK LIMITED' AS TargetName UNION ALL SELECT 'ADATIS CONSULTING LIMITED' AS SourceName, 'ADATIS CNSULTING LIMITED' AS TargetName") \
    .select(
        "SourceName", 
        "TargetName", 
        expr("jaro_winkler(SourceName, TargetName) AS JaroWinkler"), 
        expr("jaccard_sim(SourceName, TargetName) AS JaccardSimilarity"), 
        expr("1 - cosine_distance(QgramTokeniser(SourceName), QgramTokeniser(TargetName)) AS CosineDistance")
    ) \
    .display()
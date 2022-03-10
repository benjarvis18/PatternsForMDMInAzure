# Databricks notebook source
# MAGIC %md 
# MAGIC # Standardise Data
# MAGIC 
# MAGIC ## Purpose
# MAGIC Extract attributes to be used for matching and apply standardisation rules to standardise their format

# COMMAND ----------

from pyspark.sql.functions import col, expr, udf, lit
import re

# COMMAND ----------

# MAGIC %md ## Read in Companies House and Land Registry Data

# COMMAND ----------

companiesHouseDf = spark.read.parquet("/mnt/lake/BASE/CompaniesHouse")

# COMMAND ----------

landRegistryDf = spark.read.parquet("/mnt/lake/BASE/LandRegistry")

# COMMAND ----------

# MAGIC %md ## Profile the Data to Understand Common Attributes

# COMMAND ----------

companiesHouseDf.printSchema()

# COMMAND ----------

landRegistryDf.printSchema()

# COMMAND ----------

# MAGIC %md ### Attributes for Matching
# MAGIC * Organisation Name
# MAGIC * Postcode
# MAGIC * Companies House Number

# COMMAND ----------

# MAGIC %md ## Create Functions to Standardise Common Attributes

# COMMAND ----------

# MAGIC %md ### Extract Postcode from Full Address

# COMMAND ----------

import re

@udf("string")
def extractPostcodeUdf(addressString):
    return extractPostcode(addressString) 

def extractPostcode(addressString): 
    if (addressString == None):
        return None
    
    matches = re.findall(r'\b[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2}\b', addressString)
    
    if (len(matches) > 0):
        return matches[0]

# COMMAND ----------

print(extractPostcode("16 My Street, My Town, My County, PA2 6NH"))

# COMMAND ----------

# MAGIC %md ### Standardise a Postcode

# COMMAND ----------

@udf("string")
def standardisePostcodeUdf(postcode):
    return standardisePostcode(postcode) 

def standardisePostcode(postcode):
    if (postcode == None):
        return None
    
    postcode = postcode.upper().strip().replace(" ", "")
    return postcode

# COMMAND ----------

print(standardisePostcode("AB1 2CD"))

# COMMAND ----------

# MAGIC %md ### Standardise Company Name

# COMMAND ----------

def removeCharacters(string, charactersToRemove):
    rx = '[' + re.escape(''.join(charactersToRemove)) + ']'
    return re.sub(rx, '', string)

def replaceCharactersWithSpace(string, charactersToReplace):
    rx = '[' + re.escape(''.join(charactersToReplace)) + ']'
    return re.sub(rx, ' ', string)

def removeStrings(value, stringsToRemove, replaceWithSpace):  
    pattern = re.compile(r"\b(" + "|".join(stringsToRemove) + ")\\W*", re.I)

    replacement = ""

    if replaceWithSpace:
        replacement = " "

    return pattern.sub(replacement, value)

def removeStringsFromEnd(value, stringsToRemove):  
    pattern = re.compile(r"\b(" + "|".join(stringsToRemove) + ")$", re.I)  
    return pattern.sub("", value, count=1)

def removeCharactersAfterString(value, stringsToRemove):
    pattern = re.compile(r"\b(" + "|".join(stringsToRemove) + ")\\b(.*)", re.I)  
    return pattern.sub("", value)

def replaceStrings(string, replacements):
    for replacement in replacements:
        if len(replacement) <= 1:
            # If it's one character then just do a straight replacement
            string = string.replace(replacement, replacements[replacement])
        elif replacement.find("(") > 0:
            # If the replacement contains brackets then we can't match the regex using word boundaries
            regex = replacement
            string = re.sub(regex, replacements[replacement], string)
        else:
            # If it's multiple characters then do a regex replacement to ensure we only replace full words
            regex = "\\b" + replacement + "\\b"      
            string = re.sub(regex, replacements[replacement], string)

    return string


# COMMAND ----------

@udf("string")
def standardiseCompanyNameUdf(companyName):
    return standardiseCompanyName(companyName) 

def standardiseCompanyName(companyName):
    if (companyName == None):
        return None       
    
    # Remove leading and trailing spaces
    companyName = companyName.strip()

    # Convert to lowercase
    companyName = companyName.lower()
   
    # Remove the following characters from the first 3 characters in a name
    firstThreeCharacters = companyName[0:3]
    firstThreeCharacters = removeCharacters(firstThreeCharacters, ["*", "=", "#", "%", "+"])

    companyName = firstThreeCharacters + companyName[3:]

    # Replace words that have the same meaning (companies house rules)
    wordReplacements = {    
    "and": "&",
    "&": "",
    "plus": "",
    "+": "",
    "-": " ",
    ".": " ",
    ",": " ",
    "0": "o",
    "zero": "o",
    "1": "one",
    "2": "too",
    "two": "too",
    "to": "too",
    "3": "three",
    "4": "for",
    "four": "for",
    "£": "pound",
    "€": "euro",
    "$": "dollar",
    "¥": "yen",
    "%": "percentum",
    "per cent": "percentum",
    "percent": "percentum",
    "per centum": "percentum",
    "@": "at",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
    "\(the\)": " ",
    "l\.t\.d": "limited",
    "ltd": "limited",
    "public limited company": "plc",
    }

    companyName = replaceStrings(companyName, wordReplacements)

    # Remove non ascii characters
    companyName = companyName.encode("ascii", errors="ignore").decode()      
   
    # Remove invalid characters
    charactersToReplace = ["‘", "’", "'", "(", ")", "[", "]", "{", "}", "<", ">", "!", "«", "»", "“", "”", "?", ".", "/", "|", "_", "%", "*", "`", "=", "#", ";", "\\", ",", ":", "~", "£", "$", "\""]    
    companyName = replaceCharactersWithSpace(companyName, charactersToReplace)  

    # Remove the at the beginning of a name
    if companyName.startswith("the"):
        companyName = companyName[3:]

    companyName = companyName.strip()   

    # Remove company types from the end of the name 
    companyTypes = [
    "limited",
    "unlimited",
    "public limited company",
    "community interest company",
    "right to enfranchisement",
    "right to manage",
    "european economic interest grouping",
    "investment company with variable capital",
    "limited partnership",
    "limited liability partnership",
    "open-ended investment company",
    "charitable incorporated organisation",
    "industrial and provident society",
    "co-operative society",
    "community benefity society",
    "cyfyngedig",
    "anghyfyngedig",
    "cwmni cyfyngedig cyhoeddus",
    "cwmni buddiant cymunedol",
    "cwmni buddiant cymunedol cyhoeddus cyfyngedig",
    "hawl i ryddfreiniad",
    "cwmni rtm cyfyngedig",
    "cwmni buddsoddi Â chyfalaf newidiol",
    "partneriaeth cyfyngedig",
    "partneriaeth atebolrwydd cyfyngedig",
    "cwmni buddsoddiad penagored",
    "sefydliad elusennol corfforedig",
    "ltd",
    "plc",
    "cic",
    "llp",  
    "inc"
    ]

    companyTypes.sort(key=len, reverse=True)
    companyName = removeCharactersAfterString(companyName, companyTypes)  

    # Remove "the" from the end of the string
    companyName = removeStringsFromEnd(companyName, ["the"])  

    # Get rid of multiple spaces and replace with a single
    companyName = re.sub(' +',' ', companyName).strip()

    return companyName.upper()

# COMMAND ----------

print(standardiseCompanyName("Bob's Burgers Ltd."))

# COMMAND ----------

# MAGIC %md ### Standardise Company Number

# COMMAND ----------

@udf("string")
def standardiseCompanyNumberUdf(companyNumber):
    return standardiseCompanyNumber(companyNumber) 

def standardiseCompanyNumber(companyNumber):
    if (companyNumber == None):
        return None       
    
    return companyNumber.zfill(8)

# COMMAND ----------

print(standardiseCompanyNumber("1354973"))

# COMMAND ----------

# MAGIC %md ## Process Companies House

# COMMAND ----------

# MAGIC %md ### Show the Data

# COMMAND ----------

companiesHouseDf.display()

# COMMAND ----------

# MAGIC %md ### Grab the columns we need

# COMMAND ----------

companiesHouseDf = companiesHouseDf \
    .select(
        'CompanyName', 'CompanyNumber', 'RegAddress_PostCode', 'PreviousName_1_CompanyName', 'PreviousName_2_CompanyName', 'PreviousName_3_CompanyName', 'PreviousName_4_CompanyName', 'PreviousName_5_CompanyName', 'PreviousName_6_CompanyName', 'PreviousName_7_CompanyName', 'PreviousName_8_CompanyName', 'PreviousName_9_CompanyName', 'PreviousName_10_CompanyName'
    )

# COMMAND ----------

display(companiesHouseDf)

# COMMAND ----------

# MAGIC %md ### Unpivot the multiple previous name fields

# COMMAND ----------

previousNamesDf = companiesHouseDf \
    .select(
        'CompanyNumber', 'RegAddress_PostCode', 'PreviousName_1_CompanyName', 'PreviousName_2_CompanyName', 'PreviousName_3_CompanyName', 'PreviousName_4_CompanyName', 'PreviousName_5_CompanyName', 'PreviousName_6_CompanyName', 'PreviousName_7_CompanyName', 'PreviousName_8_CompanyName', 'PreviousName_9_CompanyName', 'PreviousName_10_CompanyName'
    ) \
    .select(
        "CompanyNumber",
        "RegAddress_PostCode",
        expr("stack(10, 'PreviousName1', PreviousName_1_CompanyName, 'PreviousName2', PreviousName_2_CompanyName, 'PreviousName3', PreviousName_3_CompanyName, 'PreviousName4', PreviousName_4_CompanyName, 'PreviousName5', PreviousName_5_CompanyName, 'PreviousName6', PreviousName_6_CompanyName, 'PreviousName7', PreviousName_7_CompanyName, 'PreviousName8', PreviousName_8_CompanyName, 'PreviousName9', PreviousName_9_CompanyName, 'PreviousName10', PreviousName_10_CompanyName) as (NameType, CompanyName)")
    ) \
    .filter(col("CompanyName").isNotNull()) \
    .select(
        "CompanyName",
        "CompanyNumber",
        "RegAddress_Postcode",
        lit(True).alias("IsPreviousName")
    )

# COMMAND ----------

previousNamesDf.display()

# COMMAND ----------

# MAGIC %md ### Union Current and Previous Names

# COMMAND ----------

companiesHouseDf = companiesHouseDf \
    .select(
        "CompanyName",
        "CompanyNumber",
        "RegAddress_Postcode",
        lit(False).alias("IsPreviousName")
    ) \
    .union(previousNamesDf)

# COMMAND ----------

companiesHouseDf.display()

# COMMAND ----------

# MAGIC %md ### Run Standardisation Functions

# COMMAND ----------

companiesHouseDf = companiesHouseDf \
    .select(
        standardiseCompanyNameUdf(col("CompanyName")).alias("CompanyName"),
        col("CompanyName").alias("OriginalCompanyName"),
        standardiseCompanyNumberUdf(col("CompanyNumber")).alias("CompanyNumber"),
        standardisePostcodeUdf(col("RegAddress_Postcode")).alias("Postcode"),
        col("IsPreviousName")
    )

# COMMAND ----------

companiesHouseDf.display()

# COMMAND ----------

# MAGIC %md ### Output to Data Lake

# COMMAND ----------

companiesHouseDf.write.mode("overwrite").parquet("/mnt/lake/STANDARDISED/CompaniesHouse")

# COMMAND ----------

# MAGIC %md ## Process Land Registry

# COMMAND ----------

# MAGIC %md ### Show the Data

# COMMAND ----------

landRegistryDf.display()

# COMMAND ----------

# MAGIC %md ### Grab the columns we need

# COMMAND ----------

landRegistryDf = landRegistryDf.select('TitleNumber', 'ProprietorName1', 'CompanyRegistrationNo_1', 'Proprietor1Address1')

# COMMAND ----------

landRegistryDf.display()

# COMMAND ----------

# MAGIC %md ### Run Standardisation Functions

# COMMAND ----------

landRegistryDf = landRegistryDf \
    .select(
        col("TitleNumber"),
        standardiseCompanyNameUdf(col("ProprietorName1")).alias("CompanyName"),
        col("ProprietorName1").alias("OriginalCompanyName"),
        standardiseCompanyNumberUdf(col("CompanyRegistrationNo_1")).alias("CompanyNumber"),
        standardisePostcodeUdf(extractPostcodeUdf(col("Proprietor1Address1"))).alias("Postcode")
    )

# COMMAND ----------

landRegistryDf.display()

# COMMAND ----------

# MAGIC %md ### Output to Data Lake

# COMMAND ----------

landRegistryDf.write.mode("overwrite").parquet("/mnt/lake/STANDARDISED/LandRegistry")
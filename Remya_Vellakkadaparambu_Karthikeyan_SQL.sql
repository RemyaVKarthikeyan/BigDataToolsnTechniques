-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Defining reusuable codes for Clinical Trial dataset
-- MAGIC --------------------------------------------------
-- MAGIC #Instructions: Upload the clinical trial and pharma zip file. 
-- MAGIC #Change the fileroot year (in cmd 2) to the uploaded clinicaltrial file name version year
-- MAGIC
-- MAGIC ##P.S: The last set of codes is additional analyses which works only for clinical trial 2023 as the associated columns are not in historical dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fileroot = "clinicaltrial_2023"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Extracting the year of clinical trial
-- MAGIC fileName = fileroot.split("_")
-- MAGIC trial_Year = fileName[1]
-- MAGIC trial_Year

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Copying the file to /tmp directory on driver node
-- MAGIC dbutils.fs.cp("/FileStore/tables/" + fileroot + ".zip", "file:/tmp/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Checking the fileroot.zip in DBFS directory
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Checking the fileroot.zip in local tmp directory
-- MAGIC dbutils.fs.ls("file:/tmp/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Making 'fileroot' accessible by the command line
-- MAGIC import os
-- MAGIC os.environ['fileroot'] = fileroot

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unzipping the file in the local directory

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC unzip -o -d /tmp /tmp/$fileroot.zip

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Moving the unzipped file to the DBFS directory
-- MAGIC dbutils.fs.mv("file:/tmp/" + fileroot +".csv" , "/FileStore/tables/", True )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Checking the unzipped file in DBFS directory
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Checking the DBFS directory
-- MAGIC dbutils.fs.ls ("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Listing the contents of the csv file
-- MAGIC dbutils.fs.head("/FileStore/tables/" + fileroot + ".csv" )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
-- MAGIC
-- MAGIC # Create or get a Spark session (modify appName as needed)
-- MAGIC spark = SparkSession.builder.appName("ClinicalTrialData").getOrCreate()
-- MAGIC
-- MAGIC # Define the schema for clinicaltrial_2023
-- MAGIC schema_2023 = StructType([
-- MAGIC     StructField("Id", StringType(), True),
-- MAGIC     StructField("Study_Title", StringType(), True),
-- MAGIC     StructField("Acronym", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Collaborators", StringType(), True),
-- MAGIC     StructField("Enrollment", StringType(), True),
-- MAGIC     StructField("Funder_Type", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Study_Design", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Define the schema for clinicaltrial_2020
-- MAGIC schema_2020 = StructType([
-- MAGIC     StructField("Id", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Submission", DateType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Specify the file path
-- MAGIC file_path = "/FileStore/tables/" + fileroot + ".csv"
-- MAGIC
-- MAGIC # Determine the file type and apply the appropriate schema and delimiter
-- MAGIC if "clinicaltrial_2023" in fileroot:
-- MAGIC     # Read the CSV file using the schema for clinicaltrial_2023
-- MAGIC     clinicaltrial_df = spark.read.format("csv")\
-- MAGIC         .option("delimiter", "\t")\
-- MAGIC         .option("quote","")\
-- MAGIC         .option("header", "true")\
-- MAGIC         .schema(schema_2023)\
-- MAGIC         .load(file_path)
-- MAGIC         
-- MAGIC elif "clinicaltrial_2020" in fileroot or "clinicaltrial_2021" in fileroot:
-- MAGIC     # Read the CSV file using the schema for clinicaltrial_2020
-- MAGIC     clinicaltrial_df = spark.read.format("csv")\
-- MAGIC         .option("delimiter", "|")\
-- MAGIC         .option("header", "true")\
-- MAGIC         .schema(schema_2020)\
-- MAGIC         .load(file_path)
-- MAGIC else:
-- MAGIC     print("Unknown file type in fileroot")
-- MAGIC
-- MAGIC # Display the DataFrame
-- MAGIC display(clinicaltrial_df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import trim, regexp_replace, col
-- MAGIC
-- MAGIC # Perform cleaning operations only if the fileroot is clinicaltrial_2023
-- MAGIC if "clinicaltrial_2023" in fileroot:
-- MAGIC     # Clean the first column of the DataFrame
-- MAGIC     clinicaltrial_df = clinicaltrial_df.withColumn(
-- MAGIC         clinicaltrial_df.columns[0],  # First column
-- MAGIC         regexp_replace(col(clinicaltrial_df.columns[0]), r'^"|"$', '')  # Remove leading and trailing quotation marks
-- MAGIC     ).withColumn(
-- MAGIC         clinicaltrial_df.columns[0],  # First column
-- MAGIC         trim(col(clinicaltrial_df.columns[0]))  # Trim leading and trailing spaces
-- MAGIC     )
-- MAGIC
-- MAGIC     # Clean the 14th column of the DataFrame
-- MAGIC     clinicaltrial_df = clinicaltrial_df.withColumn(
-- MAGIC         clinicaltrial_df.columns[13],  # 14th column (index 13)
-- MAGIC         regexp_replace(col(clinicaltrial_df.columns[13]), r'("|,)+$', '')  # Remove trailing quotation marks and commas
-- MAGIC     ).withColumn(
-- MAGIC         clinicaltrial_df.columns[13],  # 14th column (index 13)
-- MAGIC         trim(col(clinicaltrial_df.columns[13]))  # Trim leading and trailing spaces
-- MAGIC     )
-- MAGIC
-- MAGIC     # Display the cleaned DataFrame
-- MAGIC     display(clinicaltrial_df)
-- MAGIC else:
-- MAGIC     # If not clinicaltrial_2023, display the DataFrame as is
-- MAGIC     display(clinicaltrial_df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fileroot1= "pharma"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Copying the file to /tmp directory on driver node
-- MAGIC dbutils.fs.cp("/FileStore/tables/" + fileroot1 + ".zip", "file:/tmp/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Checking the fileroot.zip in DBFS directory
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Checking the fileroot.zip in local tmp directory
-- MAGIC dbutils.fs.ls("file:/tmp/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Making 'fileroot' accessible by the command line
-- MAGIC import os
-- MAGIC os.environ['fileroot1'] = fileroot1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unzipping the file in the local directory

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC unzip -o -d /tmp /tmp/$fileroot1.zip

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Moving the unzipped file to the DBFS directory
-- MAGIC dbutils.fs.mv("file:/tmp/" + fileroot1 +".csv" , "/FileStore/tables/", True )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Checking the unzipped file in DBFS directory
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Checking the DBFS directory
-- MAGIC dbutils.fs.ls ("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Listing the contents of the csv file
-- MAGIC dbutils.fs.head("/FileStore/tables/" + fileroot1 + ".csv" )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #reading the csv data into spark data frame using read.csv method
-- MAGIC pharmaDF = spark.read.options(delimiter = ',').csv("/FileStore/tables/" + fileroot1 + ".csv",
-- MAGIC                              header = "true",
-- MAGIC                              inferSchema = "true")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharmaDF.display()

-- COMMAND ----------

DROP TABLE IF EXISTS clinicaltrial_table;
DROP TABLE IF EXISTS pharma_table;

-- COMMAND ----------

DROP TABLE IF EXISTS pharma2023;

-- COMMAND ----------

DROP TABLE IF EXISTS clinicaltrial_table;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_df.createOrReplaceTempView("Clinicaltrial_table")
-- MAGIC pharmaDF.createOrReplaceTempView("pharma2023")

-- COMMAND ----------

SELECT *
FROM Clinicaltrial_table

-- COMMAND ----------

SELECT *
FROM pharma2023

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables

-- COMMAND ----------

CREATE OR REPLACE TABLE default.clinicaltrial_table AS
SELECT * 
FROM Clinicaltrial_table

-- COMMAND ----------

CREATE OR REPLACE TABLE default.pharma_table AS
SELECT * 
FROM pharma2023

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Question 1. The number of studies in the dataset. You must ensure that you explicitly check distinct studies.

-- COMMAND ----------

SELECT COUNT(DISTINCT Id) AS number_of_distinct_studies
FROM clinicaltrial_table;

-- COMMAND ----------

SELECT DISTINCT Id
FROM clinicaltrial_table
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Question 2. You should list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These should be ordered from most frequent to least frequent.

-- COMMAND ----------

SELECT Type, COUNT(*) AS frequency
FROM clinicaltrial_table
GROUP BY Type
ORDER BY frequency DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Question 3. The top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------

SELECT Conditions, COUNT(*) AS count
FROM clinicaltrial_table
GROUP BY Conditions
ORDER BY count DESC
LIMIT 5;


-- COMMAND ----------

WITH exploded_conditions AS (
    -- Step 1: Split the 'Conditions' column by pipe and explode
    SELECT EXPLODE(SPLIT(Conditions, '\\|')) AS Condition
    FROM clinicaltrial_table
),

condition_counts AS (
    -- Step 2: Group the exploded data by 'Condition' and count occurrences
    SELECT Condition, COUNT(*) AS count
    FROM exploded_conditions
    GROUP BY Condition
),

sorted_condition_counts AS (
    -- Step 3: Order the condition counts by count in descending order
    SELECT Condition, count
    FROM condition_counts
    ORDER BY count DESC
)

-- Step 4: Select the top 5 conditions with their frequencies
SELECT *
FROM sorted_condition_counts
LIMIT 5;

-- COMMAND ----------

WITH cleaned_conditions AS (
    -- Step 1: Remove quotes and trim leading and trailing spaces from 'Conditions' column
    SELECT TRIM(REGEXP_REPLACE(Conditions, '"', '')) AS Condition
    FROM clinicaltrial_table
),
no_brackets_conditions AS (
    -- Step 2: Replace square brackets (`[` and `]`) and parentheses (`(` and `)`) in 'Condition' column
    SELECT REGEXP_REPLACE(Condition, '[\\[\\]()]', '') AS Condition
    FROM cleaned_conditions
),
split_conditions AS (
    -- Step 3 and 4: Split 'Condition' column by pipe ('|') and explode the array
    SELECT EXPLODE(SPLIT(Condition, '\\|')) AS Condition
    FROM no_brackets_conditions
),
split_by_comma_conditions AS (
    -- Step 5 and 6: Split each condition by comma and explode the array
    SELECT EXPLODE(SPLIT(Condition, ',')) AS Condition
    FROM split_conditions
),
trimmed_conditions AS (
    -- Step 7: Trim leading and trailing whitespaces from 'Condition' column
    SELECT TRIM(Condition) AS Condition
    FROM split_by_comma_conditions
),
filtered_conditions AS (
    -- Step 8: Filter out rows with 'e.g' in a case-insensitive manner
    SELECT Condition
    FROM trimmed_conditions
    WHERE LOWER(Condition) NOT LIKE 'e.g%'
),
top_5_conditions AS (
    -- Calculate the top 5 conditions with their frequencies
    SELECT Condition, COUNT(*) AS frequency
    FROM filtered_conditions
    GROUP BY Condition
    ORDER BY frequency DESC
    LIMIT 5
)
-- Display the top 5 conditions with their frequencies
SELECT Condition, frequency
FROM top_5_conditions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Question 4. Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. Hint: For a basic implementation, you can assume that the Parent Company column contains all possible pharmaceutical companies.

-- COMMAND ----------

-- Select the Sponsor column from the clinicaltrial_table and limit to the first 5 rows
SELECT Sponsor
FROM clinicaltrial_table
LIMIT 5;

-- COMMAND ----------

-- Step 1: Retrieve a list of pharmaceutical companies from pharma_table
WITH pharma_companies AS (
    SELECT DISTINCT Parent_Company
    FROM pharma_table
)

-- Step 2: Identify non-pharmaceutical sponsors and count the number of clinical trials they have sponsored
SELECT c.Sponsor, COUNT(*) AS num_trials
FROM clinicaltrial_table AS c
-- Step 3: Left join with pharma_companies to filter out pharmaceutical sponsors
LEFT JOIN pharma_companies AS p ON c.Sponsor = p.Parent_Company
-- Step 4: Only keep non-pharmaceutical sponsors (i.e., sponsors not in the pharma_companies list)
WHERE p.Parent_Company IS NULL
-- Step 5: Group by sponsor and count the number of clinical trials they have sponsored
GROUP BY c.Sponsor
-- Step 6: Order by the count in descending order and limit the results to the top 10
ORDER BY num_trials DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Question 5. Plot number of completed studies for each month in 2023. You need to include your visualization as well as a table of all the values you have plotted for each month.(Visualized using POWER BI)

-- COMMAND ----------

-- Query to find the number of completed studies for each month in 2023
SELECT
    -- Extract the month from the Completion date and convert it to a month name (e.g., 'Jan', 'Feb', etc.)
    SUBSTR(Completion, 1, 3) AS completion_month,
    COUNT(*) AS number_of_completed_studies -- Count the number of completed studies
FROM
    clinicaltrial_table
WHERE
    -- Filter to include only rows where the status is 'Completed' and the Completion date is in 2023
    Status = 'Completed'
    AND SUBSTR(Completion, 4, 4) = '2023' -- Check if the completion year is 2023
GROUP BY
    completion_month
ORDER BY
    -- Order the results by the completion month, with month names sorted chronologically
    CASE completion_month
        WHEN 'Jan' THEN 1
        WHEN 'Feb' THEN 2
        WHEN 'Mar' THEN 3
        WHEN 'Apr' THEN 4
        WHEN 'May' THEN 5
        WHEN 'Jun' THEN 6
        WHEN 'Jul' THEN 7
        WHEN 'Aug' THEN 8
        WHEN 'Sep' THEN 9
        WHEN 'Oct' THEN 10
        WHEN 'Nov' THEN 11
        WHEN 'Dec' THEN 12
        ELSE 0
    END;


-- COMMAND ----------

-- Query to find the number of completed studies for each month in 2023
SELECT
    -- Use CASE WHEN to handle different formats of the Completion date
    CASE 
        WHEN LENGTH(Completion) = 10 THEN SUBSTR(Completion, 6, 2) -- Extract the month for yyyy-mm-dd format
        WHEN LENGTH(Completion) = 7 THEN SUBSTR(Completion, 6, 2) -- Extract the month for yyyy-mm format
        ELSE NULL -- Handle NULL values
    END AS completion_month,
    COUNT(*) AS number_of_completed_studies -- Count the number of completed studies
FROM
    clinicaltrial_table
WHERE
    -- Filter to include only rows where the status is 'COMPLETED' and the Completion date is in 2023
    Status = 'COMPLETED'
    AND (
        (LENGTH(Completion) = 10 AND SUBSTR(Completion, 1, 4) = '2023') -- yyyy-mm-dd format
        OR (LENGTH(Completion) = 7 AND SUBSTR(Completion, 1, 4) = '2023') -- yyyy-mm format
    )
GROUP BY
    -- Group the results by the completion month
    CASE 
        WHEN LENGTH(Completion) = 10 THEN SUBSTR(Completion, 6, 2) -- yyyy-mm-dd format
        WHEN LENGTH(Completion) = 7 THEN SUBSTR(Completion, 6, 2) -- yyyy-mm format
        ELSE NULL -- Handle NULL values
    END
ORDER BY
    -- Order the results by month
    completion_month;


-- COMMAND ----------

SELECT * 
FROM clinicaltrial_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Additional Analyses only for Clinical Trial 2023 (associated columns are not in historical datasets)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Analysis 1 : Distribution of clinical trials across different funder types

-- COMMAND ----------

-- Query to find the distribution of clinical trials across different funder types
SELECT
    -- Funder Type and its corresponding count of trials
    Funder_Type,
    COUNT(*) AS trial_count
FROM
    clinicaltrial_table
GROUP BY
    -- Group by Funder Type
    Funder_Type
ORDER BY
    -- Order by trial count in descending order
    trial_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Analysis 2: Trends in Clinical Trial Enrollment Over Time

-- COMMAND ----------

SELECT
    Type AS study_type,
    COUNT(*) AS total_studies,
    SUM(CASE WHEN Status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_studies
FROM
    clinicaltrial_table
GROUP BY
    Type
ORDER BY
    completed_studies DESC, total_studies DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Analysis 3: Identify the most frequent collaborators in clinical trials.

-- COMMAND ----------

SELECT
    Collaborators AS collaborator,
    COUNT(*) AS num_trials
FROM
    clinicaltrial_table
GROUP BY
    Collaborators
ORDER BY
    num_trials DESC
LIMIT 10;

-- COMMAND ----------

show tables

-- COMMAND ----------

DROP TABLE IF EXISTS clinicaltrial_table;
DROP TABLE IF EXISTS pharma_table;

-- COMMAND ----------

DROP TABLE IF EXISTS pharma2023;

-- COMMAND ----------

DROP TABLE IF EXISTS clinicaltrial_table;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define the path of the directory you want to delete
-- MAGIC directory_path = "/FileStore/tables/"
-- MAGIC
-- MAGIC # Use dbutils.fs.rm() to delete all files and directories in the specified location
-- MAGIC dbutils.fs.rm(directory_path, recurse=True)
-- MAGIC
-- MAGIC # Confirmation message
-- MAGIC print(f"All files and directories in the location {directory_path} have been deleted.")

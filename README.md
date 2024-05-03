# BigDataToolsnTechniques
Clinical Trial Data Analysis in Databricks with DataFrame, RDD, and Spark SQL Approaches

You will be using clinical trial datasets in this work and combining the information with a list of pharmaceutical companies. You will be given the answers to the questions, for a basic implementation, for two historical datasets, so you can verify your basic solution to the problems. All data will be available from Blackboard.
2.1.1.
Datasets:
The data necessary for this assignment will be zipped CSV files. The .csv files have a header describing the files’ contents. They are:
1.Clinicaltrial_2023.csv:
Every row in the dataset corresponds to an individual clinical trial and is identified by different variables. It's important to note that the first column contains a mixture of various variables separated by a delimiter, and the date columns exhibit various formats. Please consider these issues and ensure that the dataset is appropriately prepared before initiating any analysis.
(Source: ClinicalTrials.gov)
2.pharma.csv:
The file contains a small number of a publicly available list of pharmaceutical violations. For the purposes of this work, we are interested in the second column, Parent Company, which contains the name of the pharmaceutical company in question.
(Source: https://violationtracker.goodjobsfirst.org/industry/pharmaceuticals)
When creating tables for this task, you must name them as follows:

clinicaltrial_2023

pharma
The uploaded datasets, must exist (and be named) in the following locations:

/FileStore/tables/clinicaltrial_2023.csv

/FileStore/tables/pharma.csv
This is to ensure that we can run your notebooks when testing your code (marks are allocated for your code running).

You are a data scientist / AI engineer whose client wishes to gain further insight into clinical trials. You are tasked with answering these questions, using visualisations where these would support your conclusions.
You should address the following questions.
1.The number of studies in the dataset. You must ensure that you explicitly check distinct studies.

2.You should list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These should be ordered from most frequent to least frequent.

3.The top 5 conditions (from Conditions) with their frequencies.

4.Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. Hint: For a basic implementation, you can assume that the Parent Company column contains all possible pharmaceutical companies.

5.Plot number of completed studies for each month in 2023. You need to include your visualization as well as a table of all the values you have plotted for each month.

You are to implement all 5 tasks 3 times: once in Spark SQL and twice in PySpark (once in RDD and another time in DataFrame).

For the visualisation of the results, you are free to use any tool that fulfils the requirements, which can be tools such as Python’s matplotlib, Excel, Power Bi, Tableau, or any other free open-source tool you may find suitable. Using built-in visualizations directly is permitted, it will however not yield a high number of marks. Your report needs to state the software used to generate the visualization, otherwise a built-in visualization will be assumed.

2.1.3.Extra features to be implemented 

For this task, an implementation of the above which is correct, fully documented and clearly explained can only receive a maximum mark of 69% for this component. Higher scoring submissions will need to implement additional features, such as (but not limited to):

Unzipping the data inside the Databricks system (You can unzip the file on your computer before uploading it to Databricks. However, to earn extra marks, you should be able to successfully unzip it within the Databricks environment. Additionally, your code should be reusable for us, meaning it needs to include proper cleanup procedures to remove any unnecessary files and folders from the filesystem. This ensures our ability to run your code without errors.).

Maximum 3 further analyses of the data, motivated by the questions asked (new problem statements other than the above 5 problems)

Writing general and reusable code for example for different versions of data. We have provided the clinicaltrial_2020 and clinicaltrial_2021 datasets only for this purpose if you want (don’t forget, the main dataset is clinicaltrial_2023 and 2020 and 2021 versions are just for extra mark and is not compulsory to use them).

Using more advance methods to solve the problems like defining and using user defined functions.

Successfully implementing Spark functions that you have not used in the workshop.

Creation of additional visualizations presenting useful information based on your own exploration which is not covered by the problem statements.

# MS3 Challenge
Programming Challenge for MS3-Inc.

## Description
Customer X just informed us that we need to churn out a code enhancement ASAP for a new project. Here is
what they need:
1. We need a Java application that will consume a CSV file, parse the data and insert to a SQLite In-Memory
database.
    1. Table X has 10 columns A, B, C, D, E, F, G, H, I, J which correspond with the CSV file column header
names.

2. Each record needs to be verified to contain the right number of data elements to match the columns.
    2. Records that do not match the column count must be written to the bad-data-<timestamp>.csv file
    3. Elements with commas will be double quoted
3. At the end of the process write statistics to a log file
    1. \# of records received
    2. \# of records successful
    3. \# of records failed

## Steps
First, I researched about what method to use for reading and writing CSV files. 
I tried BufferedReader and other built-in methods to read CSV files but it wasn't very effective.
Trying to read a column with double quotation mark did not work as I expected with these tools. 
Later I discovered OpenCSV package and read it's documentation. It was very easy to parse CSV files into an array in java with this package.

Later, I cleaned the array from bad data with simple loops and created another separate list to hold both bad and good data.
After this step, I created three helper methods to upload the good data to SQLite, print bad data to another csv file and finally 
printing the stats on the screen and in a log file. 

-writeBadData: I used CSVWriter to write bad data array into a csv file easily.

-uploadToSQLite: I used basic SQLite methods to create new table if not exists, and PreparedStatments to dynamically construct sql statements 
and write in the the table that I created earlier. Finally, I used ResultSet to retrieve data from SQLite for debugging.

-printStats: Used PrintWriter to print stats such as # of records received, # of records successful, and # of records failed.
printed these stats on the screen and in the log file.

### Assumptions
* Given csv file has headers
* File extension is in CSV format
* The CSV file is located in the project's root folder


### Technical Info
I used Maven for package management. Used SQlite-JDBC package from org.xerial. 
Also, used OpenCSV pacakge for reading and writing CSV files. 
In order to run this program, please open this project in IntelliJ or Eclipse and run.

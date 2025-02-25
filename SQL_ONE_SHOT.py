import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] ="hadoop"
os.environ['JAVA_HOME'] = r''
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################
data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()





data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()




data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()



df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")

# Above I  am declaring multiple dataframes and will execute the queries below

#Multiple columns
spark.sql("select id,tdate from df").show()

#Single column with WHERE filter
spark.sql("select * from df where category = 'Exercise' ").show()

# IN operator - This is used when we want multiple values from a column it is a shorthand for multiple OR conditions.
spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()

#Like operator - Used in WHERE clause to search for a specified pattern in a column like finding all the mails with @gmail.com
spark.sql("select * from df where product like '%Gymnastics%'").show()

#Not Operator - to get opposite results also called negative results.
spark.sql("select * from df where category not in ('Exercise')").show()
#Multiple values with NOT operator
spark.sql("select * from df where category not in ('Exercise','Gymnastics')").show()

#NULL VALUES - IS NULL , IS NOT NULL both are used accordingly
# first one used to find the null values and the later one used to get records where values are not null
spark.sql("select * from df where product is null").show()
spark.sql("select * from df where product is not null").show()

# MIN & MAX values -- to find min and max of a column the values returns accordingly
spark.sql("select min(amount) as amount_min from df ").show()
spark.sql("select  max(amount) as amount_max from df ").show()

#Count operator -- returns the number of the rows that matches a specified criteria
spark.sql("select count(*) from df").show()
spark.sql("select count(1) from df").show() # this executes faster than * as in star we are selecting all rows but in count(1) it is setting every row to one and then combining it

#Status set - setting status of columns if condition is matched the status could be anything here we are setting 1 and 0
#it is a case when statement
spark.sql("select *, case when spendby = 'cash' then 1 else 0 end as status from df").show()
spark.sql("select *, case when spendby = 'cash' then 'NA' else 0 end as status from df").show() # SEE we can set any status and get the returning value in a column

#Concatinate two column -- adding values of two or more column with hyphen or without
spark.sql("select id,category,concat(id , '-' , category) from df as concatinated_column").show()
# but what if the columns are too many then we are not gonna declare hyphen again and again in our query so let's see how we do it then
spark.sql("select id,category,product,concat_ws('-',id,product,category) from df as concatinated_column").show(truncate=False)
#use CONCAT_WS declare hyphen one time and add values over and over without declaring hyphen again

#LOWER & UPPER CASE - representing data in good format
spark.sql("select id,category,product,lower(category) as lower_category,upper(category) as upper_category from df").show()

#CEIL Operator -- Rounding up with upper value
spark.sql("select ceil(amount) as ceil_amount from df").show()

#Round operator --Round off to the nearest integer
spark.sql("select round(amount) as round_amount from df").show()

#Replacing NULLs -- Replace it with NA or anything
#for this we use COALESCE function
spark.sql("select product,coalesce(product,'NA') as product_coalesce from df").show()

#Remove space -- sometimes users input space before entering their names and when we query the exact name we won't get it as space character is present to overcome that we use
#TRIM function
spark.sql("select product,trim(product) as product_trim from df").show()

#Disctinct -- return only distinct (different) values
spark.sql("select distinct category from df").show()

#Substring -- when we need few character from any string date maybe name, country etc
spark.sql("select substring(product,1,10) as product_substr from df").show()

#Substring with split - split data into 0 and 1 index and can inset -,_ for better readability
spark.sql("select product,split(product,' ')[0] as product_split from df").show()

#Union All - selects duplicate values and well as unique too
spark.sql("select * from df union all select * from df1").show()
#let's see union too
spark.sql("select * from df union select * from df1").show()

#Aggregate statement --total sum-up but we can do it clubbing based on columns as well
#groupby -- group rows that have same values into summary rows, used with aggregate functions like count, max, min, sum, avg.
#let's see groupby example
spark.sql("select category,sum(amount) as sum_amount from df group by category").show()

#aggregate two columns
spark.sql("select category,spendby,sum(amount) as sum_amount from df group by category,spendby").show()

#but what if i want to know how many rows got aggregated for a category
spark.sql("select category,sum(amount) as sum_amount,count(amount) as count_rows from df group by category").show()

#Aggregate MAX
spark.sql("select category,max(amount) as max_amount from df group by category").show()
##Aggregate MIN
spark.sql("select category,min(amount) as min_amount from df group by category").show()

#WINDOW FUNCTIONS - used for data analysis and db management , enabling calculations across a specific
#set of rows knows as WINDOW -- while the individual rows are retained.
# Aggregate functions that summarize data for the entire group, window functions allow detailed calculations
#for specific partitions or subsets of data.
# calculations triggers on window of data which is useful for aggregates, rankings and cumulative totals without altering the dataset.

#OVER CLAUSE IS MUST FOR DEFINING THE WINDOW & PARTITION BY DIVIDES THE DATA TO MAKE WINDOWS
#Let's look at a window function example
spark.sql("select category,amount, ROW_NUMBER() OVER (Partition by category ORDER BY amount DESC) as rowS_NUMBER from df ").show()
'''
SELECT column_name1, 
 window_function(column_name2)
 OVER([PARTITION BY column_name1] [ORDER BY column_name3]) AS new_column
FROM table_name;
window_function= any aggregate or ranking function
column_name1= column to be selected
column_name2= column on which window function is to be applied
column_name3= column on whose basis partition of rows is to be done
new_column= Name of new column
table_name= Name of table
'''

# RANK AND DENSE RANK -FUNCTIONS
#RANK() functions assign a unique rank to each distinct row in the result set, with tied rows receiving the same rank and leaving gaps in subsequent ranks.
# For example, if two rows tie for first place, the next row will receive a rank of 3.
#DENSE_RANK() also assigns ranks based on criteria, but it does not leave gaps between ranks in case of tied rows.
# This means that if there are ties, the next rank will not skip any numbers.
#RANK example
spark.sql("select category,amount, ROW_NUMBER() OVER (Partition by category ORDER BY amount DESC) as rowS_NUMBER,RANK() OVER (Partition by category ORDER BY amount DESC) as rank from df ").show()
#dense rank example
spark.sql("select category,amount, ROW_NUMBER() OVER (Partition by category ORDER BY amount DESC) as rowS_NUMBER,DENSE_RANK() OVER (Partition by category ORDER BY amount DESC) as dense_rank from df ").show()

#LEAD function
spark.sql("select category,amount,LEAD(amount) OVER (Partition by category ORDER BY amount DESC) as rowS_NUMBER from df ").show()
#LAG function
spark.sql("select category,amount, LAG(amount) OVER (Partition by category ORDER BY amount DESC) as rowS_NUMBER from df ").show()

#HAVING -- The HAVING clause in SQL is used to filter query results based on aggregate functions.
# Unlike the WHERE clause, which filters individual rows before grouping, the HAVING clause filters groups of data after aggregation.
# It is commonly used with functions like SUM(), AVG(), COUNT(), MAX(), and MIN().
#having clause example
spark.sql("select category,count(category) as counting from df group by category having count(category) > 1").show()

#JOINS -SQL JOIN clause is used to query and access data from multiple tables by establishing logical relationships between them.
#INNER JOIN -- The INNER JOIN keyword selects all rows from both the tables as long as the condition is satisfied.
# This keyword will create the result-set by combining all rows from both the tables where the condition satisfies i.e value of the common field will be the same.
#inner join example
spark.sql("select a.* , b.product from  cust a join prod b on a.id = b.id").show()
#left join
spark.sql("select a.* , b.product from  cust a left join prod b on a.id = b.id").show()
#right join
spark.sql("select a.* , b.product from  cust a right join prod b on a.id = b.id").show()
#full join
spark.sql("select a.* , b.product from  cust a full join prod b on a.id = b.id").show()
#left anti join
spark.sql("select a.* from  cust a left anti join prod b on a.id = b.id").show()



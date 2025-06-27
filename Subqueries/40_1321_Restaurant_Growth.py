# Databricks notebook source
# MAGIC %md
# MAGIC # [1321. Restaurant Growth](https://leetcode.com/problems/restaurant-growth/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Customer
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | customer_id   | int     |
# MAGIC | name          | varchar |
# MAGIC | visited_on    | date    |
# MAGIC | amount        | int     |
# MAGIC +---------------+---------+</pre>
# MAGIC In SQL,(customer_id, visited_on) is the primary key for this table.
# MAGIC This table contains data about customer transactions in a restaurant.
# MAGIC visited_on is the date on which the customer with ID (customer_id) has visited the restaurant.
# MAGIC amount is the total paid by a customer.
# MAGIC  
# MAGIC
# MAGIC You are the restaurant owner and you want to analyze a possible expansion (there will be at least one customer every day).
# MAGIC
# MAGIC Compute the moving average of how much the customer paid in a seven days window (i.e., current day + 6 days before). average_amount should be rounded to two decimal places.
# MAGIC
# MAGIC Return the result table ordered by visited_on in ascending order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Customer table:
# MAGIC <pre>+-------------+--------------+--------------+-------------+
# MAGIC | customer_id | name         | visited_on   | amount      |
# MAGIC +-------------+--------------+--------------+-------------+
# MAGIC | 1           | Jhon         | 2019-01-01   | 100         |
# MAGIC | 2           | Daniel       | 2019-01-02   | 110         |
# MAGIC | 3           | Jade         | 2019-01-03   | 120         |
# MAGIC | 4           | Khaled       | 2019-01-04   | 130         |
# MAGIC | 5           | Winston      | 2019-01-05   | 110         | 
# MAGIC | 6           | Elvis        | 2019-01-06   | 140         | 
# MAGIC | 7           | Anna         | 2019-01-07   | 150         |
# MAGIC | 8           | Maria        | 2019-01-08   | 80          |
# MAGIC | 9           | Jaze         | 2019-01-09   | 110         | 
# MAGIC | 1           | Jhon         | 2019-01-10   | 130         | 
# MAGIC | 3           | Jade         | 2019-01-10   | 150         | 
# MAGIC +-------------+--------------+--------------+-------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+--------------+--------------+----------------+
# MAGIC | visited_on   | amount       | average_amount |
# MAGIC +--------------+--------------+----------------+
# MAGIC | 2019-01-07   | 860          | 122.86         |
# MAGIC | 2019-01-08   | 840          | 120            |
# MAGIC | 2019-01-09   | 840          | 120            |
# MAGIC | 2019-01-10   | 1000         | 142.86         |
# MAGIC +--------------+--------------+----------------+</pre>
# MAGIC Explanation: 
# MAGIC 1st moving average from 2019-01-01 to 2019-01-07 has an average_amount of (100 + 110 + 120 + 130 + 110 + 140 + 150)/7 = 122.86
# MAGIC 2nd moving average from 2019-01-02 to 2019-01-08 has an average_amount of (110 + 120 + 130 + 110 + 140 + 150 + 80)/7 = 120
# MAGIC 3rd moving average from 2019-01-03 to 2019-01-09 has an average_amount of (120 + 130 + 110 + 140 + 150 + 80 + 110)/7 = 120
# MAGIC 4th moving average from 2019-01-04 to 2019-01-10 has an average_amount of (130 + 110 + 140 + 150 + 80 + 110 + 130 + 150)/7 = 142.86

# COMMAND ----------

import datetime

# pandas schema

import pandas as pd

data = [[1, 'Jhon', '2019-01-01', 100], [2, 'Daniel', '2019-01-02', 110], [3, 'Jade', '2019-01-03', 120],
        [4, 'Khaled', '2019-01-04', 130], [5, 'Winston', '2019-01-05', 110], [6, 'Elvis', '2019-01-06', 140],
        [7, 'Anna', '2019-01-07', 150], [8, 'Maria', '2019-01-08', 80], [9, 'Jaze', '2019-01-09', 110],
        [1, 'Jhon', '2019-01-10', 130], [3, 'Jade', '2019-01-10', 150]]
       # [1, 'Jhon', '2019-01-10', 130], [3, 'Jade', '2019-01-10', 150],[3, 'Jade', '2019-01-12', 150]]
customer = pd.DataFrame(data, columns=['customer_id', 'name', 'visited_on', 'amount']).astype(
    {'customer_id': 'Int64', 'name': 'object', 'visited_on': 'datetime64[ns]', 'amount': 'Int64'})

# COMMAND ----------

#to spark dataframe
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

customer_df = spark.createDataFrame(customer)
customer_df.show()

# COMMAND ----------

from pyspark.sql import Window
# Solving in Spark DataFrame
import pyspark.sql.functions as F

min_max_dates = customer_df.agg(F.min('visited_on'),F.max('visited_on'))
min_max_dates.show()

# COMMAND ----------

from datetime import timedelta

min_date = min_max_dates.collect()[0][0]
max_date = min_max_dates.collect()[0][1]

days_diff = max_date-min_date
from_min_to_max_date_list = [(min_date + timedelta(days=i),0) for i in range(0,days_diff.days+1)]

dummy_df_header = ['visited_on','amount']
dummy_df = spark.createDataFrame(from_min_to_max_date_list,dummy_df_header)
dummy_df.show()

# COMMAND ----------

# Input: Customer table:
# +-------------+--------------+--------------+-------------+
# | customer_id | name         | visited_on   | amount      |
# +-------------+--------------+--------------+-------------+
# | 1           | Jhon         | 2019-01-01   | 100         |
# Output:
# +--------------+--------------+----------------+
# | visited_on   | amount       | average_amount |
import pyspark.sql.functions as f
import pyspark.sql.window as w

## IMP NOTICE rowsBetween feature in window spec just like order and partition by
## it can limit the aggragation upto specific before/after rows ( -6,0 <start,end - both inclusive> : means 6 rows back to current row)

first_visit=customer_df.alias('c').orderBy("visited_on").select("visited_on").limit(7).orderBy(f.desc("visited_on")).limit(1).collect()[0][0]

customer_df.groupBy("visited_on") \
    .agg(f.sum("amount").alias("amount")) \
    .withColumn("amount1",f.sum('amount').over(w.Window.orderBy("visited_on").rowsBetween(-6,0))) \
    .withColumn("avg_sum",f.round(f.avg('amount').over(w.Window.orderBy("visited_on").rowsBetween(-6,0)),2)) \
    .withColumn("amount",f.col("amount1")) \
    .drop(f.col("amount1")) \
    .filter(f.col("visited_on")>=first_visit).show()


# COMMAND ----------

# Solving in spark SQL
## Logic first find the 7th day 
## Calculate total_amt and avg_amt for all days having criteria of rows between 6 pre and current row
## notice some diff syntax for rows
## then put filter on visited_on (not directly where you are calculating total/avg amts)

customer_df.createOrReplaceTempView("customers")
spark.sql(
    '''
    with cte as 
    (
        select visited_on, sum(amount) as amt_sum
        from customers 
        group by visited_on
    )
    select * from (
    select visited_on,sum(amt_sum) over(order by visited_on  rows between 6 preceding and current row) as total_amount,
            round(avg(amt_sum) over(order by visited_on rows between 6 preceding and current row),2) as avg_amt
    from cte
    ) a where visited_on>=(
          select max(visited_on) as first_visit 
          from (select visited_on from customers order by visited_on limit 7)
            )
    '''
).show()
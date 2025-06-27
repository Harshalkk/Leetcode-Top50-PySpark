# Databricks notebook source
# MAGIC %md
# MAGIC # [1907. Count Salary Categories]('https://leetcode.com/problems/count-salary-categories/description/?envType=study-plan-v2&envId=top-sql-50')

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Accounts
# MAGIC
# MAGIC <pre>+-------------+------+
# MAGIC | Column Name | Type |
# MAGIC +-------------+------+
# MAGIC | account_id  | int  |
# MAGIC | income      | int  |
# MAGIC +-------------+------+</pre>
# MAGIC account_id is the primary key (column with unique values) for this table.
# MAGIC Each row contains information about the monthly income for one bank account.
# MAGIC  
# MAGIC
# MAGIC Write a solution to calculate the number of bank accounts for each salary category. The salary categories are:
# MAGIC
# MAGIC "Low Salary": All the salaries strictly less than 20000.
# MAGIC "Average Salary": All the salaries in the inclusive range (20000, 50000).
# MAGIC "High Salary": All the salaries strictly greater than 50000.
# MAGIC
# MAGIC The result table must contain all three categories. If there are no accounts in a category, return 0.
# MAGIC
# MAGIC Return the result table in any order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Accounts table:
# MAGIC <pre>+------------+--------+
# MAGIC | account_id | income |
# MAGIC +------------+--------+
# MAGIC | 3          | 108939 |
# MAGIC | 2          | 12747  |
# MAGIC | 8          | 87709  |
# MAGIC | 6          | 91796  |
# MAGIC +------------+--------+</pre>
# MAGIC Output: 
# MAGIC <pre>+----------------+----------------+
# MAGIC | category       | accounts_count |
# MAGIC +----------------+----------------+
# MAGIC | Low Salary     | 1              |
# MAGIC | Average Salary | 0              |
# MAGIC | High Salary    | 3              |
# MAGIC +----------------+----------------+</pre>
# MAGIC Explanation: 
# MAGIC Low Salary: Account 2.
# MAGIC Average Salary: No accounts.
# MAGIC High Salary: Accounts 3, 6, and 8.

# COMMAND ----------

import pandas as pd

# Pandas schema

data = [[3, 108939], [2, 12747], [8, 87709], [6, 91796]]
accounts = pd.DataFrame(data, columns=['account_id', 'income']).astype({'account_id':'Int64', 'income':'Int64'})

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#pyspark schema

spark = SparkSession.builder.getOrCreate()

accounts_df = spark.createDataFrame(accounts)
accounts_df.show(truncate=False)

# COMMAND ----------

# in Spark Dataframe
# Input: Accounts table:
# +------------+--------+
# | account_id | income |
# +------------+--------+

# Output:
# +----------------+----------------+
# | category       | accounts_count |

## to achive Avg account with 0 count, we can have extra df ceated with all having one instance of salary class first
## and then -1 in our final query in count so 1-0=0
import pyspark.sql.functions as f

accounts_df.withColumn("category",f.when(f.col("income")<20000,'Low_Sal').when(f.col("income").between(20000,50000),"Avg_Sal").otherwise("High_Sal")) \
    .groupBy("category") \
        .agg(f.count(f.col("category")).alias("accounts_count")).show()

# COMMAND ----------

accounts_df.createOrReplaceTempView("accounts")
spark.sql(
    '''
    with cte as (
        select case when income<20000 then "Low Salary"
                    when income>=2000 and income<=50000 then "Average Salary"
                    else "High Salary"
                end as category,
                account_id
        from accounts
    )
    select category, count(account_id)-1
    from ( 
        select "Low Salary" as category, 1 as account_id
        UNION ALL select "Average Salary" as category, 1 as account_id
        UNION ALL select "High Salary" as category, 1 as account_id
        UNION ALL select * from cte
    ) a
    group by category 
    '''
).show()
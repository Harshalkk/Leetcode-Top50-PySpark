# Databricks notebook source
# MAGIC %md
# MAGIC # [1193. Monthly Transactions I](https://leetcode.com/problems/monthly-transactions-i/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Transactions
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | id            | int     |
# MAGIC | country       | varchar |
# MAGIC | state         | enum    |
# MAGIC | amount        | int     |
# MAGIC | trans_date    | date    |
# MAGIC +---------------+---------+</pre>
# MAGIC id is the primary key of this table.
# MAGIC The table has information about incoming transactions.
# MAGIC The state column is an enum of type ("approved", "declined").
# MAGIC  
# MAGIC
# MAGIC Write an SQL query to find for each month and country, the number of transactions and their total amount, the number of approved transactions and their total amount.
# MAGIC
# MAGIC Return the result table in any order.
# MAGIC
# MAGIC The query result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Transactions table:
# MAGIC <pre>+------+---------+----------+--------+------------+
# MAGIC | id   | country | state    | amount | trans_date |
# MAGIC +------+---------+----------+--------+------------+
# MAGIC | 121  | US      | approved | 1000   | 2018-12-18 |
# MAGIC | 122  | US      | declined | 2000   | 2018-12-19 |
# MAGIC | 123  | US      | approved | 2000   | 2019-01-01 |
# MAGIC | 124  | DE      | approved | 2000   | 2019-01-07 |
# MAGIC +------+---------+----------+--------+------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+----------+---------+-------------+----------------+--------------------+-----------------------+
# MAGIC | month    | country | trans_count | approved_count | trans_total_amount | approved_total_amount |
# MAGIC +----------+---------+-------------+----------------+--------------------+-----------------------+
# MAGIC | 2018-12  | US      | 2           | 1              | 3000               | 1000                  |
# MAGIC | 2019-01  | US      | 1           | 1              | 2000               | 2000                  |
# MAGIC | 2019-01  | DE      | 1           | 1              | 2000               | 2000                  |
# MAGIC +----------+---------+-------------+----------------+--------------------+-----------------------+</pre>

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[121, 'US', 'approved', 1000, '2018-12-18'], [122, 'US', 'declined', 2000, '2018-12-19'],
        [123, 'US', 'approved', 2000, '2019-01-01'], [124, 'DE', 'approved', 2000, '2019-01-07']]
transactions = pd.DataFrame(data, columns=['id', 'country', 'state', 'amount', 'trans_date']).astype(
    {'id': 'Int64', 'country': 'object', 'state': 'object', 'amount': 'Int64', 'trans_date': 'datetime64[ns]'})

# COMMAND ----------

# to spark dataframe

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

transactions_df = spark.createDataFrame(transactions)
transactions_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
# Input: Transactions table:
# +------+---------+----------+--------+------------+
# | id   | country | state    | amount | trans_date |
# Output:
# +----------+---------+-------------+----------------+--------------------+-----------------------+
# | month    | country | trans_count | approved_count | trans_total_amount | approved_total_amount |

import pyspark.sql.functions as f
transactions_df.alias('t').groupBy(f.col('country'),f.month(f.col('t.trans_date')).alias('month')) \
    .agg( f.sum(f.col('t.amount')).alias('total_trans_amount'), \
        f.count('t.country').alias('trans_count'), \
        f.count(f.when(f.col('t.state')=='approved',1).otherwise(f.lit(None))).alias('count_approved'), \
        f.sum(f.when(f.col('t.state')=='approved',f.col('t.amount')).otherwise(0)).alias('approved_amt')) \
    .show()



# COMMAND ----------

# solving in spark SQL

transactions_df.createOrReplaceTempView('trans')
spark.sql('''
          select country,month(trans_date),
            count(country) as total_trans_count,
            count(case when state="approved" then 1 end) as total_approved_count,
            sum(amount) as total_trans_amount,
            sum(case when state="approved" then amount else 0 end) as total_approved_trans
          from trans
          group by country,month(trans_date)
        ''').show()
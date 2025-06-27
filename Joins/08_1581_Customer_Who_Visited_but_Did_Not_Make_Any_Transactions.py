# Databricks notebook source
# MAGIC %md
# MAGIC # [1581. Customer Who Visited but Did Not Make Any Transactions](https://leetcode.com/problems/customer-who-visited-but-did-not-make-any-transactions/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Visits
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | visit_id    | int     |
# MAGIC | customer_id | int     |
# MAGIC +-------------+---------+</pre>
# MAGIC visit_id is the column with unique values for this table.
# MAGIC This table contains information about the customers who visited the mall.
# MAGIC  
# MAGIC
# MAGIC Table: Transactions
# MAGIC
# MAGIC <pre>+----------------+---------+
# MAGIC | Column Name    | Type    |
# MAGIC +----------------+---------+
# MAGIC | transaction_id | int     |
# MAGIC | visit_id       | int     |
# MAGIC | amount         | int     |
# MAGIC +----------------+---------+</pre>
# MAGIC transaction_id is column with unique values for this table.
# MAGIC This table contains information about the transactions made during the visit_id.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the IDs of the users who visited without making any transactions and the number of times they made these types of visits.
# MAGIC
# MAGIC Return the result table sorted in any order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Visits
# MAGIC <pre>+----------+-------------+
# MAGIC | visit_id | customer_id |
# MAGIC +----------+-------------+
# MAGIC | 1        | 23          |
# MAGIC | 2        | 9           |
# MAGIC | 4        | 30          |
# MAGIC | 5        | 54          |
# MAGIC | 6        | 96          |
# MAGIC | 7        | 54          |
# MAGIC | 8        | 54          |
# MAGIC +----------+-------------+</pre>
# MAGIC Transactions
# MAGIC <pre>+----------------+----------+--------+
# MAGIC | transaction_id | visit_id | amount |
# MAGIC +----------------+----------+--------+
# MAGIC | 2              | 5        | 310    |
# MAGIC | 3              | 5        | 300    |
# MAGIC | 9              | 5        | 200    |
# MAGIC | 12             | 1        | 910    |
# MAGIC | 13             | 2        | 970    |
# MAGIC +----------------+----------+--------+</pre>
# MAGIC Output: 
# MAGIC <pre>+-------------+----------------+
# MAGIC | customer_id | count_no_trans |
# MAGIC +-------------+----------------+
# MAGIC | 54          | 2              |
# MAGIC | 30          | 1              |
# MAGIC | 96          | 1              |
# MAGIC +-------------+----------------+</pre>
# MAGIC Explanation: 
# MAGIC Customer with id = 23 visited the mall once and made one transaction during the visit with id = 12.
# MAGIC Customer with id = 9 visited the mall once and made one transaction during the visit with id = 13.
# MAGIC Customer with id = 30 visited the mall once and did not make any transactions.
# MAGIC Customer with id = 54 visited the mall three times. During 2 visits they did not make any transactions, and during one visit they made 3 transactions.
# MAGIC Customer with id = 96 visited the mall once and did not make any transactions.
# MAGIC As we can see, users with IDs 30 and 96 visited the mall one time without making any transactions. Also, user 54 visited the mall twice and did not make any transactions.

# COMMAND ----------

#pandas schema

import pandas as pd

data = [[1, 23], [2, 9], [4, 30], [5, 54], [6, 96], [7, 54], [8, 54]]
visits = pd.DataFrame(data, columns=['visit_id', 'customer_id']).astype({'visit_id':'Int64', 'customer_id':'Int64'})
data = [[2, 5, 310], [3, 5, 300], [9, 5, 200], [12, 1, 910], [13, 2, 970]]
transactions = pd.DataFrame(data, columns=['transaction_id', 'visit_id', 'amount']).astype({'transaction_id':'Int64', 'visit_id':'Int64', 'amount':'Int64'})


# COMMAND ----------

# to pyspark schema
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

visits_df = spark.createDataFrame(visits)
visits_df.show(truncate=False)

# COMMAND ----------

transactions_df = spark.createDataFrame(transactions)
transactions_df.show(truncate=False)

# COMMAND ----------

# In spark Dataframe
### Visits visits_df
### | visit_id | customer_id |

### Transactions transaction_df
### | transaction_id | visit_id | amount 

### o/p
###  customer_id | count_no_trans 


# Join and filter after that and at last groupBy and agg
### Notice on logic
import pyspark.sql.functions as f
visits_df.alias("v").join(transactions_df.alias("t"),f.expr("v.visit_id == t.visit_id"),how="left")  \
    .filter(transactions_df.transaction_id.isNull()) \
    .groupBy(f.col("v.customer_id").alias("customer_id")).agg(f.count("v.visit_id").alias("count_no_trans")).show()



# COMMAND ----------

# In SQL
transactions_df.createOrReplaceTempView("trans")
visits_df.createOrReplaceTempView("visits")


## same logic in sql, maybe start with sql first before df join
### Notice where first and then group by
spark.sql(" select v1.customer_id,count(v1.visit_id) \
          from visits v1 left join trans t1 on v1.visit_id=t1.visit_id \
          where t1.transaction_id is null \
          group by v1.customer_id \
          ").show()

# COMMAND ----------

spark.stop()
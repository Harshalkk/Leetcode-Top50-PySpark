# Databricks notebook source
# MAGIC %md
# MAGIC # [1934. Confirmation Rate](https://leetcode.com/problems/confirmation-rate/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Signups
# MAGIC <pre>+----------------+----------+
# MAGIC | Column Name    | Type     |
# MAGIC +----------------+----------+
# MAGIC | user_id        | int      |
# MAGIC | time_stamp     | datetime |
# MAGIC +----------------+----------+</pre>
# MAGIC user_id is the column of unique values for this table.
# MAGIC Each row contains information about the signup time for the user with ID user_id.
# MAGIC  
# MAGIC
# MAGIC Table: Confirmations
# MAGIC
# MAGIC <pre>+----------------+----------+
# MAGIC | Column Name    | Type     |
# MAGIC +----------------+----------+
# MAGIC | user_id        | int      |
# MAGIC | time_stamp     | datetime |
# MAGIC | action         | ENUM     |
# MAGIC +----------------+----------+</pre>
# MAGIC (user_id, time_stamp) is the primary key (combination of columns with unique values) for this table.
# MAGIC user_id is a foreign key (reference column) to the Signups table.
# MAGIC action is an ENUM (category) of the type ('confirmed', 'timeout')
# MAGIC Each row of this table indicates that the user with ID user_id requested a confirmation message at time_stamp and that confirmation message was either confirmed ('confirmed') or expired without confirming ('timeout').
# MAGIC  
# MAGIC
# MAGIC The confirmation rate of a user is the number of 'confirmed' messages divided by the total number of requested confirmation messages. The confirmation rate of a user that did not request any confirmation messages is 0. Round the confirmation rate to two decimal places.
# MAGIC
# MAGIC Write a solution to find the confirmation rate of each user.
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
# MAGIC Signups table:
# MAGIC <pre>+---------+---------------------+
# MAGIC | user_id | time_stamp          |
# MAGIC +---------+---------------------+
# MAGIC | 3       | 2020-03-21 10:16:13 |
# MAGIC | 7       | 2020-01-04 13:57:59 |
# MAGIC | 2       | 2020-07-29 23:09:44 |
# MAGIC | 6       | 2020-12-09 10:39:37 |
# MAGIC +---------+---------------------+</pre>
# MAGIC Confirmations table:
# MAGIC <pre>+---------+---------------------+-----------+
# MAGIC | user_id | time_stamp          | action    |
# MAGIC +---------+---------------------+-----------+
# MAGIC | 3       | 2021-01-06 03:30:46 | timeout   |
# MAGIC | 3       | 2021-07-14 14:00:00 | timeout   |
# MAGIC | 7       | 2021-06-12 11:57:29 | confirmed |
# MAGIC | 7       | 2021-06-13 12:58:28 | confirmed |
# MAGIC | 7       | 2021-06-14 13:59:27 | confirmed |
# MAGIC | 2       | 2021-01-22 00:00:00 | confirmed |
# MAGIC | 2       | 2021-02-28 23:59:59 | timeout   |
# MAGIC +---------+---------------------+-----------+</pre>
# MAGIC Output: 
# MAGIC <pre>+---------+-------------------+
# MAGIC | user_id | confirmation_rate |
# MAGIC +---------+-------------------+
# MAGIC | 6       | 0.00              |
# MAGIC | 3       | 0.00              |
# MAGIC | 7       | 1.00              |
# MAGIC | 2       | 0.50              |
# MAGIC +---------+-------------------+</pre>
# MAGIC Explanation: 
# MAGIC User 6 did not request any confirmation messages. The confirmation rate is 0.
# MAGIC User 3 made 2 requests and both timed out. The confirmation rate is 0.
# MAGIC User 7 made 3 requests and all were confirmed. The confirmation rate is 1.
# MAGIC User 2 made 2 requests where one was confirmed and the other timed out. The confirmation rate is 1 / 2 = 0.5.

# COMMAND ----------

# Pandas schema

import pandas as pd

data = [[3, '2020-03-21 10:16:13'], [7, '2020-01-04 13:57:59'], [2, '2020-07-29 23:09:44'], [6, '2020-12-09 10:39:37']]
signups = pd.DataFrame(data, columns=['user_id', 'time_stamp']).astype(
    {'user_id': 'Int64', 'time_stamp': 'datetime64[ns]'})
data = [[3, '2021-01-06 03:30:46', 'timeout'], [3, '2021-07-14 14:00:00', 'timeout'],
        [7, '2021-06-12 11:57:29', 'confirmed'], [7, '2021-06-13 12:58:28', 'confirmed'],
        [7, '2021-06-14 13:59:27', 'confirmed'], [2, '2021-01-22 00:00:00', 'confirmed'],
        [2, '2021-02-28 23:59:59', 'timeout']]
confirmations = pd.DataFrame(data, columns=['user_id', 'time_stamp', 'action']).astype(
    {'user_id': 'Int64', 'time_stamp': 'datetime64[ns]', 'action': 'object'})

# COMMAND ----------

# to pyspark schema

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

signups_df = spark.createDataFrame(signups)
confirmations_df = spark.createDataFrame(confirmations)

# COMMAND ----------

signups_df.show()

# COMMAND ----------

confirmations_df.show()

# COMMAND ----------

# in Spark Dataframe
import pyspark.sql.functions as f

# notice agg function with nested round & when clauses
signups_df.alias("s1").join(confirmations_df.alias("c1"),on="user_id",how="left").groupBy(f.col("s1.user_id")) \
    .agg(f.round(f.avg(f.when(f.col("c1.action") == "confirmed",1) \
        .otherwise(0)),2).alias("confirmation_ratio")).show()

# COMMAND ----------

# in Spark SQL
signups_df.createOrReplaceTempView('signups')
confirmations_df.createOrReplaceTempView('confirmations')

spark.sql('''
select s1.user_id, 
    -- coalesce(sum(case when c1.action="confirmed" then 1 else 0 end)/count(c1.action),0)
    round(avg(case when c1.action="confirmed" then 1 else 0 end),2) as confirmation_rate
from signups s1 left join confirmations c1 on s1.user_id=c1.user_id 
group by s1.user_id
''').show()

# COMMAND ----------

spark.stop()

# COMMAND ----------


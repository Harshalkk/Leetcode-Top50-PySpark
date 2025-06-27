# Databricks notebook source
# MAGIC %md
# MAGIC # [1633. Percentage of Users Attended a Contest](https://leetcode.com/problems/percentage-of-users-attended-a-contest/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Users
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | user_id     | int     |
# MAGIC | user_name   | varchar |
# MAGIC +-------------+---------+</pre>
# MAGIC user_id is the primary key (column with unique values) for this table.
# MAGIC Each row of this table contains the name and the id of a user.
# MAGIC  
# MAGIC
# MAGIC Table: Register
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | contest_id  | int     |
# MAGIC | user_id     | int     |
# MAGIC +-------------+---------+</pre>
# MAGIC (contest_id, user_id) is the primary key (combination of columns with unique values) for this table.
# MAGIC Each row of this table contains the id of a user and the contest they registered into.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the percentage of the users registered in each contest rounded to two decimals.
# MAGIC
# MAGIC Return the result table ordered by percentage in descending order. In case of a tie, order it by contest_id in ascending order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Users table:
# MAGIC <pre>+---------+-----------+
# MAGIC | user_id | user_name |
# MAGIC +---------+-----------+
# MAGIC | 6       | Alice     |
# MAGIC | 2       | Bob       |
# MAGIC | 7       | Alex      |
# MAGIC +---------+-----------+</pre>
# MAGIC Register table:
# MAGIC <pre>+------------+---------+
# MAGIC | contest_id | user_id |
# MAGIC +------------+---------+
# MAGIC | 215        | 6       |
# MAGIC | 209        | 2       |
# MAGIC | 208        | 2       |
# MAGIC | 210        | 6       |
# MAGIC | 208        | 6       |
# MAGIC | 209        | 7       |
# MAGIC | 209        | 6       |
# MAGIC | 215        | 7       |
# MAGIC | 208        | 7       |
# MAGIC | 210        | 2       |
# MAGIC | 207        | 2       |
# MAGIC | 210        | 7       |
# MAGIC +------------+---------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------------+------------+
# MAGIC | contest_id | percentage |
# MAGIC +------------+------------+
# MAGIC | 208        | 100.0      |
# MAGIC | 209        | 100.0      |
# MAGIC | 210        | 100.0      |
# MAGIC | 215        | 66.67      |
# MAGIC | 207        | 33.33      |
# MAGIC +------------+------------+</pre>
# MAGIC Explanation: 
# MAGIC All the users registered in contests 208, 209, and 210. The percentage is 100% and we sort them in the answer table by contest_id in ascending order.
# MAGIC Alice and Alex registered in contest 215 and the percentage is ((2/3) * 100) = 66.67%
# MAGIC Bob registered in contest 207 and the percentage is ((1/3) * 100) = 33.33%

# COMMAND ----------

#pandas schema

import pandas as pd

data = [[6, 'Alice'], [2, 'Bob'], [7, 'Alex']]
users = pd.DataFrame(data, columns=['user_id', 'user_name']).astype({'user_id': 'Int64', 'user_name': 'object'})
data = [[215, 6], [209, 2], [208, 2], [210, 6], [208, 6], [209, 7], [209, 6], [215, 7], [208, 7], [210, 2], [207, 2],
        [210, 7]]
register = pd.DataFrame(data, columns=['contest_id', 'user_id']).astype({'contest_id': 'Int64', 'user_id': 'Int64'})

# COMMAND ----------

# to spark dataframe

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

users_df = spark.createDataFrame(users)
users_df.show(truncate=False)

# COMMAND ----------

register_df = spark.createDataFrame(register)
register_df.show(truncate=False)

# COMMAND ----------

# solving using spark dataframe

# Input: Users table:
# +---------+-----------+
# | user_id | user_name |
# +---------+-----------+
# | 6       | Alice   

# Register table:
# +------------+---------+
# | contest_id | user_id |
# +------------+---------+

# Output:
# +------------+------------+
# | contest_id | percentage |

### Notice there was need of total count of users table INSIDE aggregate compuation like subquery
## Otherwise can be also done this way, SO WITH COL is allowed in this and drop col as well :
        # .groupBy('contest_id').agg(F.count('user_id').alias('users_count'))\
        # .withColumn('percentage',F.round(100*F.col('users_count')/users_df.count(),2))\
                
import pyspark.sql.functions as f
users_df.alias('u').join(register_df.alias('r'),on="user_id",how="right") \
        .groupBy(f.col('r.contest_id')) \
                .agg(f.round((f.count(f.col('r.user_id'))/(users_df.count())),2).alias('percentage')) \
                        .show()

# COMMAND ----------

# solving in spark sql
users_df.createOrReplaceTempView("Users")
register_df.createOrReplaceTempView("Register")

spark.sql('''
          select r.contest_id, round(count(r.contest_id)/(select count(user_id) from Users),2) as pct
          from Users u right join Register r
                on r.user_id = u.user_id
          group by r.contest_id
          order by 2 desc
          ''').show()

# COMMAND ----------

spark.stop()
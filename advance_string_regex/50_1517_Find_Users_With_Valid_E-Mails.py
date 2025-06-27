# Databricks notebook source
# MAGIC %md
# MAGIC # [1517. Find Users With Valid E-Mails](https://leetcode.com/problems/find-users-with-valid-e-mails/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Users
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | user_id       | int     |
# MAGIC | name          | varchar |
# MAGIC | mail          | varchar |
# MAGIC +---------------+---------+</pre>
# MAGIC user_id is the primary key (column with unique values) for this table.
# MAGIC This table contains information of the users signed up in a website. Some e-mails are invalid.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the users who have valid emails.
# MAGIC
# MAGIC A valid e-mail has a prefix name and a domain where:
# MAGIC
# MAGIC The prefix name is a string that may contain letters (upper or lower case), digits, underscore '_', period '.', and/or dash '-'. The prefix name must start with a letter.
# MAGIC The domain is '@leetcode.com'.
# MAGIC Return the result table in any order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Users table:
# MAGIC <pre>+---------+-----------+-------------------------+
# MAGIC | user_id | name      | mail                    |
# MAGIC +---------+-----------+-------------------------+
# MAGIC | 1       | Winston   | winston@leetcode.com    |
# MAGIC | 2       | Jonathan  | jonathanisgreat         |
# MAGIC | 3       | Annabelle | bella-@leetcode.com     |
# MAGIC | 4       | Sally     | sally.come@leetcode.com |
# MAGIC | 5       | Marwan    | quarz#2020@leetcode.com |
# MAGIC | 6       | David     | david69@gmail.com       |
# MAGIC | 7       | Shapiro   | .shapo@leetcode.com     |
# MAGIC +---------+-----------+-------------------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+---------+-----------+-------------------------+
# MAGIC | user_id | name      | mail                    |
# MAGIC +---------+-----------+-------------------------+
# MAGIC | 1       | Winston   | winston@leetcode.com    |
# MAGIC | 3       | Annabelle | bella-@leetcode.com     |
# MAGIC | 4       | Sally     | sally.come@leetcode.com |
# MAGIC +---------+-----------+-------------------------+</pre>
# MAGIC Explanation: 
# MAGIC The mail of user 2 does not have a domain.
# MAGIC The mail of user 5 has the # sign which is not allowed.
# MAGIC The mail of user 6 does not have the leetcode domain.
# MAGIC The mail of user 7 starts with a period.

# COMMAND ----------

# Pandas schema

import pandas as pd

data = [[1, 'Winston', 'winston@leetcode.com'], [2, 'Jonathan', 'jonathanisgreat'],
        [3, 'Annabelle', 'bella-@leetcode.com'], [4, 'Sally', 'sally.come@leetcode.com'],
        [5, 'Marwan', 'quarz#2020@leetcode.com'], [6, 'David', 'david69@gmail.com'],
        [7, 'Shapiro', '.shapo@leetcode.com']]
users = pd.DataFrame(data, columns=['user_id', 'name', 'mail']).astype(
    {'user_id': 'int64', 'name': 'object', 'mail': 'object'})

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

#pyspark schema

spark = SparkSession.builder.getOrCreate()

users_df = spark.createDataFrame(users)
users_df.show(truncate=False)

# COMMAND ----------

# Solving in spark dataframe
import pyspark.sql.functions as f

## Notice there is rlike function for regex matching

# digits, underscore '_', period '.', and/or dash '-'. T
users_df \
    .filter(f.col("mail").rlike('^[A-Za-z]+[-a-zA-Z.0-9_]*@leetcode\\.com$')).show(truncate=False)

# COMMAND ----------

# Solving in Spark SQL

users_df.createOrReplaceTempView('users')
spark.sql(
    '''
    select * from users
    where mail regexp("^[a-zA-z]+[-a-zA-Z.0-9_]*@leetcode\\.com$")
    '''
).show(truncate=False)

# COMMAND ----------

spark.stop()
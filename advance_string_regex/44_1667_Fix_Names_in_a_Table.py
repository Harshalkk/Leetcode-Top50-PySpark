# Databricks notebook source
# MAGIC %md
# MAGIC # [1667. Fix Names in a Table](https://leetcode.com/problems/fix-names-in-a-table/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Users
# MAGIC
# MAGIC <pre>+----------------+---------+
# MAGIC | Column Name    | Type    |
# MAGIC +----------------+---------+
# MAGIC | user_id        | int     |
# MAGIC | name           | varchar |
# MAGIC +----------------+---------+</pre>
# MAGIC user_id is the primary key (column with unique values) for this table.
# MAGIC This table contains the ID and the name of the user. The name consists of only lowercase and uppercase characters.
# MAGIC  
# MAGIC
# MAGIC Write a solution to fix the names so that only the first character is uppercase and the rest are lowercase.
# MAGIC
# MAGIC Return the result table ordered by user_id.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Users table:
# MAGIC <pre>+---------+-------+
# MAGIC | user_id | name  |
# MAGIC +---------+-------+
# MAGIC | 1       | aLice |
# MAGIC | 2       | bOB   |
# MAGIC +---------+-------+</pre>
# MAGIC Output: 
# MAGIC <pre>+---------+-------+
# MAGIC | user_id | name  |
# MAGIC +---------+-------+
# MAGIC | 1       | Alice |
# MAGIC | 2       | Bob   |
# MAGIC +---------+-------+</pre>

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[1, 'aLice'], [2, 'bOB']]
users = pd.DataFrame(data, columns=['user_id', 'name']).astype({'user_id':'Int64', 'name':'object'})

# COMMAND ----------

# converting to spark dataframe

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

users_df = spark.createDataFrame(users)
users_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe

## usercap() function to captialise first letter of each word
import pyspark.sql.functions as f
users_df.withColumn("name",f.initcap(f.col("name"))).show()

# COMMAND ----------

users_df.createOrReplaceTempView("users")

## Possibly there will be same function in sql like initcap()
spark.sql('''
          select user_id,initcap(name) as name
          from users
          ''').show()
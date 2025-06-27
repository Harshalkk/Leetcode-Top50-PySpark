# Databricks notebook source
# MAGIC %md
# MAGIC # [1683. Invalid Tweets](https://leetcode.com/problems/invalid-tweets/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Tweets
# MAGIC <pre>
# MAGIC +----------------+---------+
# MAGIC | Column Name    | Type    |
# MAGIC +----------------+---------+
# MAGIC | tweet_id       | int     |
# MAGIC | content        | varchar |
# MAGIC +----------------+---------+</pre>
# MAGIC tweet_id is the primary key (column with unique values) for this table.
# MAGIC This table contains all the tweets in a social media app.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the IDs of the invalid tweets. The tweet is invalid if the number of characters used in the content of the tweet is strictly greater than 15.
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
# MAGIC Tweets table:
# MAGIC <pre>
# MAGIC +----------+----------------------------------+
# MAGIC | tweet_id | content                          |
# MAGIC +----------+----------------------------------+
# MAGIC | 1        | Vote for Biden                   |
# MAGIC | 2        | Let us make America great again! |
# MAGIC +----------+----------------------------------+</pre>
# MAGIC Output: 
# MAGIC <pre>
# MAGIC +----------+
# MAGIC | tweet_id |
# MAGIC +----------+
# MAGIC | 2        |
# MAGIC +----------+</pre>
# MAGIC Explanation: 
# MAGIC Tweet 1 has length = 14. It is a valid tweet.
# MAGIC Tweet 2 has length = 32. It is an invalid tweet.

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as f


# creating panda dataframe
## data in line & columns don't have dtypes
pdf=pd.DataFrame(data=[[1,"Vote for Biden"], [2,"Let us make America great again!"]],columns=["tweet_id","content"])#.astype=({"tweet_id":int,"content":object})

## astype can't be chained above line ???
pdf=pdf.astype({"tweet_id":int,"content":object}) 

# creating spark df using panda df
sdf=spark.createDataFrame(data=pdf)

# filter
sdf.filter(f.length(f.col("content"))>15).select(f.column("tweet_id")).show()
           

# COMMAND ----------

# Using Sql
sdf.createOrReplaceTempView("Tweets")

## Define query variable & use it in spark.sql
query="select tweet_id from Tweets where len(content)>15"
spark.sql(query).display()

# COMMAND ----------


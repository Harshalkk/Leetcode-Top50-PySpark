# Databricks notebook source
# MAGIC %md
# MAGIC # [1148. Article Views I](https://leetcode.com/problems/article-views-i/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Views
# MAGIC <pre>
# MAGIC +---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | article_id    | int     |
# MAGIC | author_id     | int     |
# MAGIC | viewer_id     | int     |
# MAGIC | view_date     | date    |
# MAGIC +---------------+---------+
# MAGIC </pre>
# MAGIC There is no primary key (column with unique values) for this table, the table may have duplicate rows.
# MAGIC Each row of this table indicates that some viewer viewed an article (written by some author) on some date. 
# MAGIC Note that equal author_id and viewer_id indicate the same person.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find all the authors that viewed at least one of their own articles.
# MAGIC
# MAGIC Return the result table sorted by id in ascending order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Views table:
# MAGIC <pre>
# MAGIC +------------+-----------+-----------+------------+
# MAGIC | article_id | author_id | viewer_id | view_date  |
# MAGIC +------------+-----------+-----------+------------+
# MAGIC | 1          | 3         | 5         | 2019-08-01 |
# MAGIC | 1          | 3         | 6         | 2019-08-02 |
# MAGIC | 2          | 7         | 7         | 2019-08-01 |
# MAGIC | 2          | 7         | 6         | 2019-08-02 |
# MAGIC | 4          | 7         | 1         | 2019-07-22 |
# MAGIC | 3          | 4         | 4         | 2019-07-21 |
# MAGIC | 3          | 4         | 4         | 2019-07-21 |
# MAGIC +------------+-----------+-----------+------------+</pre>
# MAGIC Output: 
# MAGIC <pre>
# MAGIC +------+
# MAGIC | id   |
# MAGIC +------+
# MAGIC | 4    |
# MAGIC | 7    |
# MAGIC +------+</pre>

# COMMAND ----------

import pyspark.sql.functions as f
# article_id | author_id | viewer_id | view_date  |
# +------------+-----------+-----------+------------+
# | 1          | 3         | 5         | 2019-08-01 |
# | 1          | 3         | 6         | 2019-08-02 |
# | 2          | 7         | 7         | 2019-08-01 |
# | 2          | 7         | 6         | 2019-08-02 |
# | 4          | 7         | 1         | 2019-07-22 |
# | 3          | 4         | 4         | 2019-07-21 |
# | 3          | 4         | 4         | 2019-07-21 |
# +------------+-----------+-----------+------------+

# creating data
data=[[1,3,5],
      [1,3,6],
      [2,7,7],
      [2,7,6],
      [4,7,1],
      [3,4,4],
      [3,4,4]]

# creating df with array data
df=spark.createDataFrame(data=data,schema=["article_id","author_id","viewer_id"])
df.display()

#Filtering
## using f.col -> import spark.sql.functions as f
df.filter(f.col('author_id')==f.col('viewer_id')).select(f.column('author_id')).distinct().show()

# COMMAND ----------

# In SQL
df.createOrReplaceTempView("journals")
spark.sql("select distinct author_id from journals where author_id==viewer_id").show()
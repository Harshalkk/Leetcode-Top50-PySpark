# Databricks notebook source
# MAGIC %md
# MAGIC # [1757. Recyclable and Low Fat Products](https://leetcode.com/problems/recyclable-and-low-fat-products/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Products
# MAGIC
# MAGIC <pre>
# MAGIC +-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | product_id  | int     |
# MAGIC | low_fats    | enum    |
# MAGIC | recyclable  | enum    |
# MAGIC +-------------+---------+</pre>
# MAGIC
# MAGIC product_id is the primary key (column with unique values) for this table.
# MAGIC low_fats is an ENUM (category) of type ('Y', 'N') where 'Y' means this product is low fat and 'N' means it is not.
# MAGIC recyclable is an ENUM (category) of types ('Y', 'N') where 'Y' means this product is recyclable and 'N' means it is not.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the ids of products that are both low fat and recyclable.
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
# MAGIC Products table:
# MAGIC <pre>
# MAGIC +-------------+----------+------------+
# MAGIC | product_id  | low_fats | recyclable |
# MAGIC +-------------+----------+------------+
# MAGIC | 0           | Y        | N          |
# MAGIC | 1           | Y        | Y          |
# MAGIC | 2           | N        | Y          |
# MAGIC | 3           | Y        | Y          |
# MAGIC | 4           | N        | N          |
# MAGIC +-------------+----------+------------+
# MAGIC </pre>
# MAGIC Output:
# MAGIC <pre> 
# MAGIC +-------------+
# MAGIC | product_id  |
# MAGIC +-------------+
# MAGIC | 1           |
# MAGIC | 3           |
# MAGIC +-------------+
# MAGIC </pre>
# MAGIC Explanation: Only products 1 and 3 are both low fat and recyclable.

# COMMAND ----------

# Pandas Schema

import pandas as pd ## this is cool
import numpy as np

# creating panda df from array data or numpy array
#data=np.array([[0,'Y','N'],[1,'Y','Y'],[2,'N','Y'],[3,'Y','Y'],[4,'N','N']])
data1=[[0,'Y','N'],[1,'Y','Y'],[2,'N','Y'],[3,'Y','Y'],[4,'N','N']]
pdf=pd.DataFrame(data1,columns=['product_id','low_fats','recyclable'])

# Creating pyspark df
df=spark.createDataFrame(pdf)

# filtering with where (or filter - same syntax) and then select
## notice round bracket for each condition and single & or |
filtered_df=df.where((df['low_fats']=='Y') & (df['recyclable']=='Y')).select('product_id')
display(filtered_df)

# COMMAND ----------

# Using SQL
## Notice no output from createOrReplace and directly using spark.sql & viewName provided previously
df.createOrReplaceTempView("Products")
a=spark.sql("select product_id from Products where low_fats=='Y' AND recyclable=='Y'")
a.show()

# COMMAND ----------

# in Spark SQL

products_df.createOrReplaceTempView('Products')
spark.sql('''
select product_id from products where low_fats="Y" and recyclable="Y";
''').show()
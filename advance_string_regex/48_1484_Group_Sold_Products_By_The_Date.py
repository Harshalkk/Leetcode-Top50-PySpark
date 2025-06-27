# Databricks notebook source
# MAGIC %md
# MAGIC # [1484. Group Sold Products By The Date](https://leetcode.com/problems/group-sold-products-by-the-date/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table Activities:
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | sell_date   | date    |
# MAGIC | product     | varchar |
# MAGIC +-------------+---------+</pre>
# MAGIC There is no primary key (column with unique values) for this table. It may contain duplicates.
# MAGIC Each row of this table contains the product name and the date it was sold in a market.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find for each date the number of different products sold and their names.
# MAGIC
# MAGIC The sold products names for each date should be sorted lexicographically.
# MAGIC
# MAGIC Return the result table ordered by sell_date.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Activities table:
# MAGIC <pre>+------------+------------+
# MAGIC | sell_date  | product     |
# MAGIC +------------+------------+
# MAGIC | 2020-05-30 | Headphone  |
# MAGIC | 2020-06-01 | Pencil     |
# MAGIC | 2020-06-02 | Mask       |
# MAGIC | 2020-05-30 | Basketball |
# MAGIC | 2020-06-01 | Bible      |
# MAGIC | 2020-06-02 | Mask       |
# MAGIC | 2020-05-30 | T-Shirt    |
# MAGIC +------------+------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------------+----------+------------------------------+
# MAGIC | sell_date  | num_sold | products                     |
# MAGIC +------------+----------+------------------------------+
# MAGIC | 2020-05-30 | 3        | Basketball,Headphone,T-shirt |
# MAGIC | 2020-06-01 | 2        | Bible,Pencil                 |
# MAGIC | 2020-06-02 | 1        | Mask                         |
# MAGIC +------------+----------+------------------------------+</pre>
# MAGIC Explanation: 
# MAGIC For 2020-05-30, Sold items were (Headphone, Basketball, T-shirt), we sort them lexicographically and separate them by a comma.
# MAGIC For 2020-06-01, Sold items were (Pencil, Bible), we sort them lexicographically and separate them by a comma.
# MAGIC For 2020-06-02, the Sold item is (Mask), we just return it.

# COMMAND ----------

# pandas schema

import pandas as pd

data = [['2020-05-30', 'Headphone'], ['2020-06-01', 'Pencil'], ['2020-06-02', 'Mask'], ['2020-05-30', 'Basketball'],
        ['2020-06-01', 'Bible'], ['2020-06-02', 'Mask'], ['2020-05-30', 'T-Shirt']]
activities = pd.DataFrame(data, columns=['sell_date', 'product']).astype(
    {'sell_date': 'datetime64[ns]', 'product': 'object'})

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

#pyspark schema

spark = SparkSession.builder.getOrCreate()

activities_df = spark.createDataFrame(activities)
activities_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe

# Input: Activities table:
# +------------+------------+
# | sell_date  | product     |
# +------------+------------+
# | 2020-05-30 | Headphone  |
# Output:
# +------------+----------+------------------------------+
# | sell_date  | num_sold | products                     |
# +------------+----------+------------------------------+
# | 2020-05-30 | 3        | Basketball,Headphone,T-shirt |

import pyspark.sql.functions as f
activities_df.groupBy("sell_date") \
  .agg(f.sort_array(f.collect_set(f.col("product"))).alias("Products") \
            ,f.countDistinct("product").alias("units_sold") \
      ).show(truncate=False)
  

# COMMAND ----------

# solving in spark SQL

activities_df.createOrReplaceTempView("products")
spark.sql('''
          select sell_date,count( distinct product) as num_sold, sort_array(collect_set(product)) as products
          from products
          group by sell_date
          ''').show(truncate=False)

# COMMAND ----------

spark.stop()
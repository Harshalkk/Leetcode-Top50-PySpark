# Databricks notebook source
# MAGIC %md
# MAGIC # [1327. List the Products Ordered in a Period](https://leetcode.com/problems/list-the-products-ordered-in-a-period/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Products
# MAGIC
# MAGIC <pre>+------------------+---------+
# MAGIC | Column Name      | Type    |
# MAGIC +------------------+---------+
# MAGIC | product_id       | int     |
# MAGIC | product_name     | varchar |
# MAGIC | product_category | varchar |
# MAGIC +------------------+---------+</pre>
# MAGIC product_id is the primary key (column with unique values) for this table.
# MAGIC This table contains data about the company's products.
# MAGIC  
# MAGIC
# MAGIC Table: Orders
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | product_id    | int     |
# MAGIC | order_date    | date    |
# MAGIC | unit          | int     |
# MAGIC +---------------+---------+</pre>
# MAGIC This table may have duplicate rows.
# MAGIC product_id is a foreign key (reference column) to the Products table.
# MAGIC unit is the number of products ordered in order_date.
# MAGIC  
# MAGIC
# MAGIC Write a solution to get the names of products that have at least 100 units ordered in February 2020 and their amount.
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
# MAGIC <pre>+-------------+-----------------------+------------------+
# MAGIC | product_id  | product_name          | product_category |
# MAGIC +-------------+-----------------------+------------------+
# MAGIC | 1           | Leetcode Solutions    | Book             |
# MAGIC | 2           | Jewels of Stringology | Book             |
# MAGIC | 3           | HP                    | Laptop           |
# MAGIC | 4           | Lenovo                | Laptop           |
# MAGIC | 5           | Leetcode Kit          | T-shirt          |
# MAGIC +-------------+-----------------------+------------------+</pre>
# MAGIC Orders table:
# MAGIC <pre>+--------------+--------------+----------+
# MAGIC | product_id   | order_date   | unit     |
# MAGIC +--------------+--------------+----------+
# MAGIC | 1            | 2020-02-05   | 60       |
# MAGIC | 1            | 2020-02-10   | 70       |
# MAGIC | 2            | 2020-01-18   | 30       |
# MAGIC | 2            | 2020-02-11   | 80       |
# MAGIC | 3            | 2020-02-17   | 2        |
# MAGIC | 3            | 2020-02-24   | 3        |
# MAGIC | 4            | 2020-03-01   | 20       |
# MAGIC | 4            | 2020-03-04   | 30       |
# MAGIC | 4            | 2020-03-04   | 60       |
# MAGIC | 5            | 2020-02-25   | 50       |
# MAGIC | 5            | 2020-02-27   | 50       |
# MAGIC | 5            | 2020-03-01   | 50       |
# MAGIC +--------------+--------------+----------+</pre>
# MAGIC Output: 
# MAGIC <pre>+--------------------+---------+
# MAGIC | product_name       | unit    |
# MAGIC +--------------------+---------+
# MAGIC | Leetcode Solutions | 130     |
# MAGIC | Leetcode Kit       | 100     |
# MAGIC +--------------------+---------+</pre>
# MAGIC Explanation: 
# MAGIC Products with product_id = 1 is ordered in February a total of (60 + 70) = 130.
# MAGIC Products with product_id = 2 is ordered in February a total of 80.
# MAGIC Products with product_id = 3 is ordered in February a total of (2 + 3) = 5.
# MAGIC Products with product_id = 4 was not ordered in February 2020.
# MAGIC Products with product_id = 5 is ordered in February a total of (50 + 50) = 100.

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[1, 'Leetcode Solutions', 'Book'], [2, 'Jewels of Stringology', 'Book'], [3, 'HP', 'Laptop'],
        [4, 'Lenovo', 'Laptop'], [5, 'Leetcode Kit', 'T-shirt']]
products = pd.DataFrame(data, columns=['product_id', 'product_name', 'product_category']).astype(
    {'product_id': 'Int64', 'product_name': 'object', 'product_category': 'object'})
data = [[1, '2020-02-05', 60], [1, '2020-02-10', 70], [2, '2020-01-18', 30], [2, '2020-02-11', 80],
        [3, '2020-02-17', 2], [3, '2020-02-24', 3], [4, '2020-03-01', 20], [4, '2020-03-04', 30], [4, '2020-03-04', 60],
        [5, '2020-02-25', 50], [5, '2020-02-27', 50], [5, '2020-03-01', 50]]
orders = pd.DataFrame(data, columns=['product_id', 'order_date', 'unit']).astype(
    {'product_id': 'Int64', 'order_date': 'datetime64[ns]', 'unit': 'Int64'})

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

#pyspark schema

spark = SparkSession.builder.getOrCreate()

products_df = spark.createDataFrame(products)
products_df.show(truncate=False)

# COMMAND ----------

orders_df = spark.createDataFrame(orders)
orders_df.show(truncate=False)

# COMMAND ----------

# Input: Products table:
# +-------------+-----------------------+------------------+
# | product_id  | product_name          | product_category |
# +-------------+-----------------------+------------------+
# | 1           | Leetcode Solutions    | Book             |

# Orders table:
# +--------------+--------------+----------+
# | product_id   | order_date   | unit     |
# +--------------+--------------+----------+
# | 1            | 2020-02-05   | 60       |

import pyspark.sql.functions as f
import pyspark.sql.window as w

products_df.join(orders_df.alias("o"),"product_id") \
    .filter(f.col("order_date").between("2020-02-01","2020-02-29")) \
        .groupBy("product_name") \
            .agg(f.sum("unit").alias("Products_sold")) \
            .filter(f.col("Products_sold")>=100).show()

# COMMAND ----------

# Solving in Spark SQL

orders_df.createOrReplaceTempView("orders")
products_df.createOrReplaceTempView("products")

spark.sql(
    '''
    select product_name, sum(unit) as units_sold
    from products natural join orders
    where order_date between '2020-02-01' and '2020-02-29'
    group by product_name
    having sum(unit) >= 100
    '''
).show()
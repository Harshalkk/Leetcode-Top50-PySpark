# Databricks notebook source
# MAGIC %md
# MAGIC # [1164. Product Price at a Given Date](https://leetcode.com/problems/product-price-at-a-given-date/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Products
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | product_id    | int     |
# MAGIC | new_price     | int     |
# MAGIC | change_date   | date    |
# MAGIC +---------------+---------+</pre>
# MAGIC (product_id, change_date) is the primary key (combination of columns with unique values) of this table.
# MAGIC Each row of this table indicates that the price of some product was changed to a new price at some date.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the prices of all products on 2019-08-16. Assume the price of all products before any change is 10.
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
# MAGIC <pre>+------------+-----------+-------------+
# MAGIC | product_id | new_price | change_date |
# MAGIC +------------+-----------+-------------+
# MAGIC | 1          | 20        | 2019-08-14  |
# MAGIC | 2          | 50        | 2019-08-14  |
# MAGIC | 1          | 30        | 2019-08-15  |
# MAGIC | 1          | 35        | 2019-08-16  |
# MAGIC | 2          | 65        | 2019-08-17  |
# MAGIC | 3          | 20        | 2019-08-18  |
# MAGIC +------------+-----------+-------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------------+-------+
# MAGIC | product_id | price |
# MAGIC +------------+-------+
# MAGIC | 2          | 50    |
# MAGIC | 1          | 35    |
# MAGIC | 3          | 10    |
# MAGIC +------------+-------+</pre>

# COMMAND ----------

# Pandas Schema
import pandas as pd

data = [[1, 20, '2019-08-14'], [2, 50, '2019-08-14'], [1, 30, '2019-08-15'], [1, 35, '2019-08-16'],
        [2, 65, '2019-08-17'], [3, 20, '2019-08-18']]

# data = [ #test case 4 from leet code
#     [1, 20, '2019-08-17'],
#     [2, 50, '2019-08-18'],
#     [1, 30, '2019-08-15'],
#     [1, 35, '2019-08-16'],
#     [2, 65, '2019-08-17'],
#     [3, 20, '2019-08-18']
# ]

products = pd.DataFrame(data, columns=['product_id', 'new_price', 'change_date']).astype(
    {'product_id': 'Int64', 'new_price': 'Int64', 'change_date': 'datetime64[ns]'})

# COMMAND ----------

# to spark schema

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

products_df = spark.createDataFrame(products)
products_df.show(truncate=False)

# COMMAND ----------

# In spark dataframe
import pyspark.sql.window as w 
import pyspark.sql.functions as f
# Done it with 2 diff dfs and then combined them using unionAll
#  Logic: first filtered using row_number function as need only highest
#  In second : only remiaing product_ids are considered using anti join and given constant price as 10 - Using f.lit(10)
# 

pdf1=products_df.filter(f.col("change_date")<="2019-08-16") \
    .withColumn("rn",f.row_number().over(w.Window.partitionBy('product_id').orderBy(f.desc("change_date")))) \
    .filter(f.col("rn")==1) \
        .select("product_id","new_price")


pdf2=products_df.alias("p1").join(pdf1.alias("p2"),on=["product_id"],how="anti") \
    .select("product_id",f.lit(10))

pdf1.unionAll(pdf2).show()


# COMMAND ----------

products_df.createOrReplaceTempView("products")

spark.sql(
    '''
    with cte as 
    (
        select *,row_number() over (partition by product_id order by change_date desc) as rn
        from products
        where change_date<='2019-08-16'
    )
    , cte2 (
        select product_id,new_price 
        from cte where rn=1
    )
    select product_id,10
    from products p1 anti join cte2 p2 on p1.product_id = p2.product_id
    UNION ALL
    select * from cte2
'''
).show()
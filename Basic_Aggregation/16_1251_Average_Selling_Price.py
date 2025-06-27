# Databricks notebook source
# MAGIC %md
# MAGIC # [1251. Average Selling Price](https://leetcode.com/problems/average-selling-price/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Prices
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | product_id    | int     |
# MAGIC | start_date    | date    |
# MAGIC | end_date      | date    |
# MAGIC | price         | int     |
# MAGIC +---------------+---------+</pre>
# MAGIC (product_id, start_date, end_date) is the primary key (combination of columns with unique values) for this table.
# MAGIC Each row of this table indicates the price of the product_id in the period from start_date to end_date.
# MAGIC For each product_id there will be no two overlapping periods. That means there will be no two intersecting periods for the same product_id.
# MAGIC  
# MAGIC
# MAGIC Table: UnitsSold
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | product_id    | int     |
# MAGIC | purchase_date | date    |
# MAGIC | units         | int     |
# MAGIC +---------------+---------+</pre>
# MAGIC This table may contain duplicate rows.
# MAGIC Each row of this table indicates the date, units, and product_id of each product sold. 
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the average selling price for each product. average_price should be rounded to 2 decimal places.
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
# MAGIC Prices table:
# MAGIC <pre>+------------+------------+------------+--------+
# MAGIC | product_id | start_date | end_date   | price  |
# MAGIC +------------+------------+------------+--------+
# MAGIC | 1          | 2019-02-17 | 2019-02-28 | 5      |
# MAGIC | 1          | 2019-03-01 | 2019-03-22 | 20     |
# MAGIC | 2          | 2019-02-01 | 2019-02-20 | 15     |
# MAGIC | 2          | 2019-02-21 | 2019-03-31 | 30     |
# MAGIC +------------+------------+------------+--------+</pre>
# MAGIC UnitsSold table:
# MAGIC <pre>+------------+---------------+-------+
# MAGIC | product_id | purchase_date | units |
# MAGIC +------------+---------------+-------+
# MAGIC | 1          | 2019-02-25    | 100   |
# MAGIC | 1          | 2019-03-01    | 15    |
# MAGIC | 2          | 2019-02-10    | 200   |
# MAGIC | 2          | 2019-03-22    | 30    |
# MAGIC +------------+---------------+-------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------------+---------------+
# MAGIC | product_id | average_price |
# MAGIC +------------+---------------+
# MAGIC | 1          | 6.96          |
# MAGIC | 2          | 16.96         |
# MAGIC +------------+---------------+</pre>
# MAGIC Explanation: 
# MAGIC Average selling price = Total Price of Product / Number of products sold.
# MAGIC Average selling price for product 1 = ((100 * 5) + (15 * 20)) / 115 = 6.96
# MAGIC Average selling price for product 2 = ((200 * 15) + (30 * 30)) / 230 = 16.96

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[1, '2019-02-17', '2019-02-28', 5], [1, '2019-03-01', '2019-03-22', 20], [2, '2019-02-01', '2019-02-20', 15],
        [2, '2019-02-21', '2019-03-31', 30]]
prices = pd.DataFrame(data, columns=['product_id', 'start_date', 'end_date', 'price']).astype(
    {'product_id': 'Int64', 'start_date': 'datetime64[ns]', 'end_date': 'datetime64[ns]', 'price': 'Int64'})
data = [[1, '2019-02-25', 100], [1, '2019-03-01', 15], [2, '2019-02-10', 200], [2, '2019-03-22', 30]]
units_sold = pd.DataFrame(data, columns=['product_id', 'purchase_date', 'units']).astype(
    {'product_id': 'Int64', 'purchase_date': 'datetime64[ns]', 'units': 'Int64'})

# COMMAND ----------

#to pyspark df

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

prices_df = spark.createDataFrame(prices)
prices_df.show(truncate=False)

# COMMAND ----------

units_sold_df = spark.createDataFrame(units_sold)
units_sold_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
import pyspark.sql.functions as f

# product_id | start_date | end_date   | price  |
# product_id | purchase_date | units
#o/p | product_id | average_price |

## Use f.* functions in agg, if you see error like col is not iterable
prices_df.alias('p').join(units_sold_df.alias('u'), (f.col('p.product_id') == f.col('u.product_id')) & \
    (f.col('u.purchase_date').between(f.col('p.start_date'),f.col('p.end_date'))),how='left') \
        .groupBy(f.col('p.product_id')) \
            .agg((f.round((f.sum(f.col('u.units') * f.col('p.price')))/f.sum('u.units'),2)).alias('average_price')).show() 



# COMMAND ----------

# solving in Spark SQL

units_sold_df.createOrReplaceTempView("units_sold")
prices_df.createOrReplaceTempView("prices")

spark.sql('''
          select p.product_id, round(sum(p.price*u.units)/sum(u.units),2) as average_price
          from prices p left join units_sold u 
            on p.product_id = u.product_id 
                AND u.purchase_date between p.start_date and p.end_date 
          GROUP BY p.product_id
        ''').show()
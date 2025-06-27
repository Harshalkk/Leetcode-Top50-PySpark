# Databricks notebook source
# MAGIC %md
# MAGIC # [1068. Product Sales Analysis I](https://leetcode.com/problems/product-sales-analysis-i/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Sales
# MAGIC <pre>
# MAGIC +-------------+-------+
# MAGIC | Column Name | Type  |
# MAGIC +-------------+-------+
# MAGIC | sale_id     | int   |
# MAGIC | product_id  | int   |
# MAGIC | year        | int   |
# MAGIC | quantity    | int   |
# MAGIC | price       | int   |
# MAGIC +-------------+-------+</pre>
# MAGIC (sale_id, year) is the primary key (combination of columns with unique values) of this table.
# MAGIC product_id is a foreign key (reference column) to Product table.
# MAGIC Each row of this table shows a sale on the product product_id in a certain year.
# MAGIC Note that the price is per unit.
# MAGIC  
# MAGIC
# MAGIC Table: Product
# MAGIC <pre>
# MAGIC +--------------+---------+
# MAGIC | Column Name  | Type    |
# MAGIC +--------------+---------+
# MAGIC | product_id   | int     |
# MAGIC | product_name | varchar |
# MAGIC +--------------+---------+</pre>
# MAGIC product_id is the primary key (column with unique values) of this table.
# MAGIC Each row of this table indicates the product name of each product.
# MAGIC  
# MAGIC
# MAGIC Write a solution to report the product_name, year, and price for each sale_id in the Sales table.
# MAGIC
# MAGIC Return the resulting table in any order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Sales table:
# MAGIC <pre>+---------+------------+------+----------+-------+
# MAGIC | sale_id | product_id | year | quantity | price |
# MAGIC +---------+------------+------+----------+-------+ 
# MAGIC | 1       | 100        | 2008 | 10       | 5000  |
# MAGIC | 2       | 100        | 2009 | 12       | 5000  |
# MAGIC | 7       | 200        | 2011 | 15       | 9000  |
# MAGIC +---------+------------+------+----------+-------+</pre>
# MAGIC Product table:
# MAGIC <pre>+------------+--------------+
# MAGIC | product_id | product_name |
# MAGIC +------------+--------------+
# MAGIC | 100        | Nokia        |
# MAGIC | 200        | Apple        |
# MAGIC | 300        | Samsung      |
# MAGIC +------------+--------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+--------------+-------+-------+
# MAGIC | product_name | year  | price |
# MAGIC +--------------+-------+-------+
# MAGIC | Nokia        | 2008  | 5000  |
# MAGIC | Nokia        | 2009  | 5000  |
# MAGIC | Apple        | 2011  | 9000  |
# MAGIC +--------------+-------+-------+</pre>
# MAGIC Explanation: 
# MAGIC From sale_id = 1, we can conclude that Nokia was sold for 5000 in the year 2008.
# MAGIC From sale_id = 2, we can conclude that Nokia was sold for 5000 in the year 2009.
# MAGIC From sale_id = 7, we can conclude that Apple was sold for 9000 in the year 2011.

# COMMAND ----------

#pandas schema
import pandas as pd

data = [[1, 100, 2008, 10, 5000], [2, 100, 2009, 12, 5000], [7, 200, 2011, 15, 9000]]
sales = pd.DataFrame(data, columns=['sale_id', 'product_id', 'year', 'quantity', 'price']).astype({'sale_id':'Int64', 'product_id':'Int64', 'year':'Int64', 'quantity':'Int64', 'price':'Int64'})
data = [[100, 'Nokia'], [200, 'Apple'], [300, 'Samsung']]
product = pd.DataFrame(data, columns=['product_id', 'product_name']).astype({'product_id':'Int64', 'product_name':'object'})

# COMMAND ----------

#to pyspark schema

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sales_df = spark.createDataFrame(sales)
sales_df.show(truncate=False)

# COMMAND ----------

product_df = spark.createDataFrame(product)
product_df.show(truncate=False)

# COMMAND ----------

# in Spark Dataframe
import pyspark.sql.functions as f
### product_df, sales_df
### product_df:  product_id | product_name 
### sales_df: sale_id|product_id|year|quantity|price|
### o/p: product_name,year,price

# JOIN
## Notice select - it accepts multiple cols as array and also accepts alias.colName
## We can give f.expr(colName as newColName) to select
sales_df.alias("s").join(product_df.alias("p"),(f.col('s.product_id')==f.col('p.product_id')),how="inner").select(["p.product_name","s.year",f.expr("s.price as total_price")]).show()


# COMMAND ----------

# In SQL
sales_df.createOrReplaceTempView("sales")
product_df.createOrReplaceTempView("product")

spark.sql("select p.product_name,s.year,s.price as total_price \
    from sales s join product p on s.product_id=p.product_id").show()
# Databricks notebook source
# MAGIC %md
# MAGIC # [1174. Immediate Food Delivery II](https://leetcode.com/problems/immediate-food-delivery-ii/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Delivery
# MAGIC
# MAGIC <pre>+-----------------------------+---------+
# MAGIC | Column Name                 | Type    |
# MAGIC +-----------------------------+---------+
# MAGIC | delivery_id                 | int     |
# MAGIC | customer_id                 | int     |
# MAGIC | order_date                  | date    |
# MAGIC | customer_pref_delivery_date | date    |
# MAGIC +-----------------------------+---------+</pre>
# MAGIC delivery_id is the column of unique values of this table.
# MAGIC The table holds information about food delivery to customers that make orders at some date and specify a preferred delivery date (on the same order date or after it).
# MAGIC  
# MAGIC
# MAGIC If the customer's preferred delivery date is the same as the order date, then the order is called immediate; otherwise, it is called scheduled.
# MAGIC
# MAGIC The first order of a customer is the order with the earliest order date that the customer made. It is guaranteed that a customer has precisely one first order.
# MAGIC
# MAGIC Write a solution to find the percentage of immediate orders in the first orders of all customers, rounded to 2 decimal places.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Delivery table:
# MAGIC <pre>+-------------+-------------+------------+-----------------------------+
# MAGIC | delivery_id | customer_id | order_date | customer_pref_delivery_date |
# MAGIC +-------------+-------------+------------+-----------------------------+
# MAGIC | 1           | 1           | 2019-08-01 | 2019-08-02                  |
# MAGIC | 2           | 2           | 2019-08-02 | 2019-08-02                  |
# MAGIC | 3           | 1           | 2019-08-11 | 2019-08-12                  |
# MAGIC | 4           | 3           | 2019-08-24 | 2019-08-24                  |
# MAGIC | 5           | 3           | 2019-08-21 | 2019-08-22                  |
# MAGIC | 6           | 2           | 2019-08-11 | 2019-08-13                  |
# MAGIC | 7           | 4           | 2019-08-09 | 2019-08-09                  |
# MAGIC +-------------+-------------+------------+-----------------------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+----------------------+
# MAGIC | immediate_percentage |
# MAGIC +----------------------+
# MAGIC | 50.00                |
# MAGIC +----------------------+</pre>
# MAGIC Explanation: 
# MAGIC The customer id 1 has a first order with delivery id 1 and it is scheduled.
# MAGIC The customer id 2 has a first order with delivery id 2 and it is immediate.
# MAGIC The customer id 3 has a first order with delivery id 5 and it is scheduled.
# MAGIC The customer id 4 has a first order with delivery id 7 and it is immediate.
# MAGIC Hence, half the customers have immediate first orders.

# COMMAND ----------

#Pandas schema

import pandas as pd

data = [[1, 1, '2019-08-01', '2019-08-02'], [2, 2, '2019-08-02', '2019-08-02'], [3, 1, '2019-08-11', '2019-08-12'],
        [4, 3, '2019-08-24', '2019-08-24'], [5, 3, '2019-08-21', '2019-08-22'], [6, 2, '2019-08-11', '2019-08-13'],
        [7, 4, '2019-08-09', '2019-08-09']]
delivery = pd.DataFrame(data,
                        columns=['delivery_id', 'customer_id', 'order_date', 'customer_pref_delivery_date']).astype(
    {'delivery_id': 'Int64', 'customer_id': 'Int64', 'order_date': 'datetime64[ns]',
     'customer_pref_delivery_date': 'datetime64[ns]'})

# COMMAND ----------

# to spark dataframe

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

delivery_df = spark.createDataFrame(delivery)
delivery_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
# Input: Delivery table:
# +-------------+-------------+------------+-----------------------------+
# | delivery_id | customer_id | order_date | customer_pref_delivery_date |
#o/p
#  immediate_percentage |
import pyspark.sql.functions as f
import pyspark.sql.window as w

firstO=w.Window().partitionBy('customer_id').orderBy('order_date')
no_of_imm_order=delivery_df.alias('d').withColumn('order_no',f.row_number().over(firstO)) \
  .filter((f.col('order_no')==1) & (f.col('order_date')==f.col('customer_pref_delivery_date')))\
    .count()

total_orders=delivery_df.alias('d').select(delivery_df['customer_id']).distinct().count()
print(no_of_imm_order/total_orders)

# COMMAND ----------

# solving in spark SQL

## Case statement in aggregation is really imp to understand especially sum or count(case ** )
delivery_df.createOrReplaceTempView('delivery')
spark.sql('''
          with cte as (
              select customer_id,order_date,customer_pref_delivery_date,
              row_number() over (partition by customer_id order by order_date) as rn
              from delivery
          )
          select count(case when cte.order_date = cte.customer_pref_delivery_date then 1 else null end) / count(*)
          from cte 
          where cte.rn = 1 
          ''').show()

# COMMAND ----------

spark.stop()
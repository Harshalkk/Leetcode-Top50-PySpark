# Databricks notebook source
# MAGIC %md
# MAGIC # [1211. Queries Quality and Percentage](https://leetcode.com/problems/queries-quality-and-percentage/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Queries
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | query_name  | varchar |
# MAGIC | result      | varchar |
# MAGIC | position    | int     |
# MAGIC | rating      | int     |
# MAGIC +-------------+---------+</pre>
# MAGIC This table may have duplicate rows.
# MAGIC This table contains information collected from some queries on a database.
# MAGIC The position column has a value from 1 to 500.
# MAGIC The rating column has a value from 1 to 5. Query with rating less than 3 is a poor query.
# MAGIC  
# MAGIC
# MAGIC We define query quality as:
# MAGIC
# MAGIC The average of the ratio between query rating and its position.
# MAGIC
# MAGIC We also define poor query percentage as:
# MAGIC
# MAGIC The percentage of all queries with rating less than 3.
# MAGIC
# MAGIC Write a solution to find each query_name, the quality and poor_query_percentage.
# MAGIC
# MAGIC Both quality and poor_query_percentage should be rounded to 2 decimal places.
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
# MAGIC Queries table:
# MAGIC <pre>+------------+-------------------+----------+--------+
# MAGIC | query_name | result            | position | rating |
# MAGIC +------------+-------------------+----------+--------+
# MAGIC | Dog        | Golden Retriever  | 1        | 5      |
# MAGIC | Dog        | German Shepherd   | 2        | 5      |
# MAGIC | Dog        | Mule              | 200      | 1      |
# MAGIC | Cat        | Shirazi           | 5        | 2      |
# MAGIC | Cat        | Siamese           | 3        | 3      |
# MAGIC | Cat        | Sphynx            | 7        | 4      |
# MAGIC +------------+-------------------+----------+--------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------------+---------+-----------------------+
# MAGIC | query_name | quality | poor_query_percentage |
# MAGIC +------------+---------+-----------------------+
# MAGIC | Dog        | 2.50    | 33.33                 |
# MAGIC | Cat        | 0.66    | 33.33                 |
# MAGIC +------------+---------+-----------------------+</pre>
# MAGIC Explanation: 
# MAGIC Dog queries quality is ((5 / 1) + (5 / 2) + (1 / 200)) / 3 = 2.50
# MAGIC Dog queries poor_ query_percentage is (1 / 3) * 100 = 33.33
# MAGIC
# MAGIC Cat queries quality equals ((2 / 5) + (3 / 3) + (4 / 7)) / 3 = 0.66
# MAGIC Cat queries poor_ query_percentage is (1 / 3) * 100 = 33.33

# COMMAND ----------

#pandas schema
import pandas as pd

data = [['Dog', 'Golden Retriever', 1, 5], ['Dog', 'German Shepherd', 2, 5], ['Dog', 'Mule', 200, 1],
        ['Cat', 'Shirazi', 5, 2], ['Cat', 'Siamese', 3, 3], ['Cat', 'Sphynx', 7, 4]]
queries = pd.DataFrame(data, columns=['query_name', 'result', 'position', 'rating']).astype(
    {'query_name': 'object', 'result': 'object', 'position': 'Int64', 'rating': 'Int64'})

# COMMAND ----------

#to pyspark df

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

queries_df = spark.createDataFrame(queries)
queries_df.show(truncate=False)

# COMMAND ----------


### Notice we use when and otherwise
import pyspark.sql.functions as f
queries_df.alias('q').groupBy('query_name') \
    .agg(f.round((f.avg(f.col('rating')/f.col('position'))),2).alias('quality'), \
        f.round(f.avg(f.when(f.col('rating')<3,1).otherwise(0)),2).alias('pct')) \
            .show()

# COMMAND ----------

# solving in spark dataframe
# Queries table:
# +------------+-------------------+----------+--------+
# | query_name | result            | position | rating |
# +------------+-------------------+----------+--------+
# | Dog        | Golden Retriever  | 1        | 5      |

# Output:
# +------------+---------+-----------------------+
# | query_name | quality | poor_query_percentage |

# Notice we used only agg function without + * directly
queries_df.createOrReplaceTempView('queries')
spark.sql('''
        select query_name,
            round(avg(rating/position),2) as quality,
            round(avg(case when rating<3 then 1 else 0 end ),2) as pct
        from queries
        group by query_name 
        ''').show()

# COMMAND ----------

# solving in spark sql

queries_df.createOrReplaceTempView('queries')

spark.sql('''
select 
    query_name, 
    round(avg(rating/position),2) as quality,
    round(100*avg(case when rating<3 then 1 else 0 end),2) as poor_query_percentage
from queries
group by query_name;
''').show()
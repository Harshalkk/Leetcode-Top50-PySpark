# Databricks notebook source
# MAGIC %md
# MAGIC # [180. Consecutive Numbers](https://leetcode.com/problems/consecutive-numbers/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Logs
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | id          | int     |
# MAGIC | num         | varchar |
# MAGIC +-------------+---------+</pre>
# MAGIC In SQL, id is the primary key for this table.
# MAGIC id is an autoincrement column.
# MAGIC  
# MAGIC
# MAGIC Find all numbers that appear at least three times consecutively.
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
# MAGIC Logs table:
# MAGIC <pre>+----+-----+
# MAGIC | id | num |
# MAGIC +----+-----+
# MAGIC | 1  | 1   |
# MAGIC | 2  | 1   |
# MAGIC | 3  | 1   |
# MAGIC | 4  | 2   |
# MAGIC | 5  | 1   |
# MAGIC | 6  | 2   |
# MAGIC | 7  | 2   |
# MAGIC +----+-----+</pre>
# MAGIC Output: 
# MAGIC <pre>+-----------------+
# MAGIC | ConsecutiveNums |
# MAGIC +-----------------+
# MAGIC | 1               |
# MAGIC +-----------------+</pre>
# MAGIC Explanation: 1 is the only number that appears consecutively for at least three times.

# COMMAND ----------

#pandas schema

import pandas as pd

data = [[1, 1], [2, 1], [3, 1], [4, 2], [5, 1], [6, 2], [7, 2]]
logs = pd.DataFrame(data, columns=['id', 'num']).astype({'id':'Int64', 'num':'Int64'})

# COMMAND ----------

# to spark schema

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

logs_df = spark.createDataFrame(logs)
logs_df.show(truncate=False)

# COMMAND ----------

# in Spark Dataframe
import pyspark.sql.window as w
import pyspark.sql.functions as f

## logic ==>
## lead & lag calculated 
## middle one will get matched as: last_no = next_no (Yeah seems like can't calculate more than 3 consecutive)
## & then lead = current number 

logs_df \
    .withColumn("lead", \
        f.lead("num").over(w.Window.orderBy("id"))) \
    .withColumn("lag", \
        f.lag("num").over(w.Window.orderBy("id"))) \
     .filter((f.col('lead')==f.col('lag')) & (f.col('lead') == f.col('num'))) \
        .select("num").show()
    

# COMMAND ----------

# in Spark SQL
logs_df.createOrReplaceTempView("logs")

spark.sql(
    '''
    with cte as (
        select num,lead(num,1) over(order by id) as lead1 , lag(num,1) over(order by id) as lag1
        from logs
    )
    select num
    from cte 
    where lead1=lag1 and lead1=num
    '''
).show()
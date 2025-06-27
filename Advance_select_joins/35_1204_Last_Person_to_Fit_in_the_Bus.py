# Databricks notebook source
# MAGIC %md
# MAGIC # [1204. Last Person to Fit in the Bus](https://leetcode.com/problems/last-person-to-fit-in-the-bus/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Queue
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | person_id   | int     |
# MAGIC | person_name | varchar |
# MAGIC | weight      | int     |
# MAGIC | turn        | int     |
# MAGIC +-------------+---------+</pre>
# MAGIC person_id column contains unique values.
# MAGIC This table has the information about all people waiting for a bus.
# MAGIC The person_id and turn columns will contain all numbers from 1 to n, where n is the number of rows in the table.
# MAGIC turn determines the order of which the people will board the bus, where turn=1 denotes the first person to board and turn=n denotes the last person to board.
# MAGIC weight is the weight of the person in kilograms.
# MAGIC  
# MAGIC
# MAGIC There is a queue of people waiting to board a bus. However, the bus has a weight limit of 1000 kilograms, so there may be some people who cannot board.
# MAGIC
# MAGIC Write a solution to find the person_name of the last person that can fit on the bus without exceeding the weight limit. The test cases are generated such that the first person does not exceed the weight limit.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Queue table:
# MAGIC <pre>+-----------+-------------+--------+------+
# MAGIC | person_id | person_name | weight | turn |
# MAGIC +-----------+-------------+--------+------+
# MAGIC | 5         | Alice       | 250    | 1    |
# MAGIC | 4         | Bob         | 175    | 5    |
# MAGIC | 3         | Alex        | 350    | 2    |
# MAGIC | 6         | John Cena   | 400    | 3    |
# MAGIC | 1         | Winston     | 500    | 6    |
# MAGIC | 2         | Marie       | 200    | 4    |
# MAGIC +-----------+-------------+--------+------+</pre>
# MAGIC Output: 
# MAGIC <pre>+-------------+
# MAGIC | person_name |
# MAGIC +-------------+
# MAGIC | John Cena   |
# MAGIC +-------------+</pre>
# MAGIC Explanation: The folowing table is ordered by the turn for simplicity.
# MAGIC <pre>+------+----+-----------+--------+--------------+
# MAGIC | Turn | ID | Name      | Weight | Total Weight |
# MAGIC +------+----+-----------+--------+--------------+
# MAGIC | 1    | 5  | Alice     | 250    | 250          |
# MAGIC | 2    | 3  | Alex      | 350    | 600          |
# MAGIC | 3    | 6  | John Cena | 400    | 1000         | (last person to board)
# MAGIC | 4    | 2  | Marie     | 200    | 1200         | (cannot board)
# MAGIC | 5    | 4  | Bob       | 175    | ___          |
# MAGIC | 6    | 1  | Winston   | 500    | ___          |
# MAGIC +------+----+-----------+--------+--------------+</pre>

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[5, 'Alice', 250, 1], [4, 'Bob', 175, 5], [3, 'Alex', 350, 2], [6, 'John Cena', 400, 3], [1, 'Winston', 500, 6],
        [2, 'Marie', 200, 4]]
queue = pd.DataFrame(data, columns=['person_id', 'person_name', 'weight', 'turn']).astype(
    {'person_id': 'Int64', 'person_name': 'object', 'weight': 'Int64', 'turn': 'Int64'})

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

queue_df = spark.createDataFrame(queue)
queue_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql import window
from pyspark.sql import functions as f

# Input: Queue table:
# +-----------+-------------+--------+------+
# | person_id | person_name | weight | turn |
# Output:
# +-------------+
# | person_name |

## Used sum in Window function and got first element whose running_sum <=1000
queue_df \
    .withColumn("running_sum",f.sum("weight").over(window.Window.orderBy("turn"))) \
    .filter(f.col("running_sum")<=1000) \
    .orderBy(f.desc("running_sum")) \
    .limit(1). \
    select("person_name").show()


# COMMAND ----------

# In spark sql

queue_df.createOrReplaceTempView("queue")
spark.sql(
    '''
    with cte(
        select *, sum(weight) over(order by turn) as running_sum
        from queue
    )
    select person_name
    from cte
    where running_sum<=1000
    order by running_sum desc
    limit 1
    '''
).show()

# COMMAND ----------


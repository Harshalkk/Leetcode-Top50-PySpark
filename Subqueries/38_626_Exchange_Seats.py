# Databricks notebook source
# MAGIC %md
# MAGIC # [626. Exchange Seats](https://leetcode.com/problems/exchange-seats/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Seat
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | id          | int     |
# MAGIC | student     | varchar |
# MAGIC +-------------+---------+</pre>
# MAGIC id is the primary key (unique value) column for this table.
# MAGIC Each row of this table indicates the name and the ID of a student.
# MAGIC id is a continuous increment.
# MAGIC  
# MAGIC
# MAGIC Write a solution to swap the seat id of every two consecutive students. If the number of students is odd, the id of the last student is not swapped.
# MAGIC
# MAGIC Return the result table ordered by id in ascending order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Seat table:
# MAGIC <pre>+----+---------+
# MAGIC | id | student |
# MAGIC +----+---------+
# MAGIC | 1  | Abbot   |
# MAGIC | 2  | Doris   |
# MAGIC | 3  | Emerson |
# MAGIC | 4  | Green   |
# MAGIC | 5  | Jeames  |
# MAGIC +----+---------+</pre>
# MAGIC Output: 
# MAGIC <pre>+----+---------+
# MAGIC | id | student |
# MAGIC +----+---------+
# MAGIC | 1  | Doris   |
# MAGIC | 2  | Abbot   |
# MAGIC | 3  | Green   |
# MAGIC | 4  | Emerson |
# MAGIC | 5  | Jeames  |
# MAGIC +----+---------+</pre>
# MAGIC Explanation: 
# MAGIC Note that if the number of students is odd, there is no need to change the last one's seat.

# COMMAND ----------

#pandas schema

import pandas as pd

data = [[1, 'Abbot'], [2, 'Doris'], [3, 'Emerson'], [4, 'Green'], [5, 'Jeames']]
seat = pd.DataFrame(data, columns=['id', 'student']).astype({'id': 'Int64', 'student': 'object'})

# COMMAND ----------

#to spark dataframe

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

seat_df = spark.createDataFrame(seat)
seat_df.show()

# COMMAND ----------

import pyspark.sql.functions as f

## Logic :
## first fetched max(id)
## created new col with 
# when max_id then id
# if id is even then id-1
# else id is odd then id+1
## return student and this new col renamed

max_id=seat_df.agg(f.max("id")).collect()[0][0]

seat_df.alias('s1') \
.withColumn("new_id",f.when(f.col('id')==max_id,f.col('id'))\
                     .when(f.col('id')%2==0,f.col('id')-1) \
                     .when(f.col('id')%2!=0,f.col('id')+1)) \
.select(f.expr("new_id as id"),"student") \
.orderBy("id").show()

# COMMAND ----------

seat_df.createOrReplaceTempView("seats")

spark.sql(
    '''
    with cte as (
        select max(id) as max_id
        from seats
    )
    select student, case
                        when id=cte.max_id and id%2==1 then id
                        when id%2=0 then id-1
                        else id+1
                    end as new_id
    from seats join cte on 1=1
    order by new_id
'''
).show()
# Databricks notebook source
# MAGIC %md
# MAGIC # [602. Friend Requests II: Who Has the Most Friends](https://leetcode.com/problems/friend-requests-ii-who-has-the-most-friends/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: RequestAccepted
# MAGIC
# MAGIC <pre>+----------------+---------+
# MAGIC | Column Name    | Type    |
# MAGIC +----------------+---------+
# MAGIC | requester_id   | int     |
# MAGIC | accepter_id    | int     |
# MAGIC | accept_date    | date    |
# MAGIC +----------------+---------+</pre>
# MAGIC (requester_id, accepter_id) is the primary key (combination of columns with unique values) for this table.
# MAGIC This table contains the ID of the user who sent the request, the ID of the user who received the request, and the date when the request was accepted.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the people who have the most friends and the most friends number.
# MAGIC
# MAGIC The test cases are generated so that only one person has the most friends.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC RequestAccepted table:
# MAGIC <pre>+--------------+-------------+-------------+
# MAGIC | requester_id | accepter_id | accept_date |
# MAGIC +--------------+-------------+-------------+
# MAGIC | 1            | 2           | 2016/06/03  |
# MAGIC | 1            | 3           | 2016/06/08  |
# MAGIC | 2            | 3           | 2016/06/08  |
# MAGIC | 3            | 4           | 2016/06/09  |
# MAGIC +--------------+-------------+-------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+----+-----+
# MAGIC | id | num |
# MAGIC +----+-----+
# MAGIC | 3  | 3   |
# MAGIC +----+-----+</pre>
# MAGIC Explanation: 
# MAGIC The person with id 3 is a friend of people 1, 2, and 4, so he has three friends in total, which is the most number than any others.
# MAGIC  
# MAGIC
# MAGIC Follow up: In the real world, multiple people could have the same most number of friends. Could you find all these people in this case?

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[1, 2, '2016/06/03'], [1, 3, '2016/06/08'], [2, 3, '2016/06/08'], [3, 4, '2016/06/09']]
request_accepted = pd.DataFrame(data, columns=['requester_id', 'accepter_id', 'accept_date']).astype(
    {'requester_id': 'Int64', 'accepter_id': 'Int64', 'accept_date': 'datetime64[ns]'})

# COMMAND ----------

#to spark dataframe
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

request_accepted_df = spark.createDataFrame(request_accepted)
request_accepted_df.show()

# COMMAND ----------

# solving in spark df
import pyspark.sql.functions as f

# Input: RequestAccepted table:
# +--------------+-------------+-------------+
# | requester_id | accepter_id | accept_date |

# Output:
# +----+-----+
# | id | num |

## Calculated total_friends using group by on both col seperately and renamed col requester/accepter to id 
## USed unionALL to combine 2 dfs

r1=request_accepted_df.groupBy("requester_id") \
    .agg(f.count("accepter_id").alias("total_friends")) \
            .withColumnRenamed("requester_id","id")

r2=request_accepted_df.groupBy("accepter_id") \
    .agg(f.count("requester_id").alias("total_friends")) \
        .withColumnRenamed("accepter_id","id")

r1.unionAll(r2) \
 .groupBy("id") \
    .agg(f.sum("total_friends").alias("total_friends")) \
        .orderBy(f.desc("total_friends")) \
            .limit(1).show()


# COMMAND ----------

# Solving in Spark SQL

request_accepted_df.createOrReplaceTempView("requests")

spark.sql(
    '''
with cte1 as 
(
    select requester_id as id , count(accepter_id) as total_friends
    from requests 
    group by requester_id
),
cte2 as(
    select accepter_id as id, count(requester_id) as total_friends
    from requests 
    group by accepter_id
)
select id,sum(total_friends) as total_friends1
from 
(
    select id, total_friends 
    from cte1
    UNION
    select id,total_friends
    from cte2
)
group by id
order by sum(total_friends) desc
limit 1

''').show()

# COMMAND ----------

spark.stop()
# Databricks notebook source
# MAGIC %md
# MAGIC # [1661. Average Time of Process per Machine](https://leetcode.com/problems/average-time-of-process-per-machine/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Activity
# MAGIC
# MAGIC <pre>+----------------+---------+
# MAGIC | Column Name    | Type    |
# MAGIC +----------------+---------+
# MAGIC | machine_id     | int     |
# MAGIC | process_id     | int     |
# MAGIC | activity_type  | enum    |
# MAGIC | timestamp      | float   |
# MAGIC +----------------+---------+</pre>
# MAGIC The table shows the user activities for a factory website.
# MAGIC (machine_id, process_id, activity_type) is the primary key (combination of columns with unique values) of this table.
# MAGIC machine_id is the ID of a machine.
# MAGIC process_id is the ID of a process running on the machine with ID machine_id.
# MAGIC activity_type is an ENUM (category) of type ('start', 'end').
# MAGIC timestamp is a float representing the current time in seconds.
# MAGIC 'start' means the machine starts the process at the given timestamp and 'end' means the machine ends the process at the given timestamp.
# MAGIC The 'start' timestamp will always be before the 'end' timestamp for every (machine_id, process_id) pair.
# MAGIC  
# MAGIC
# MAGIC There is a factory website that has several machines each running the same number of processes. Write a solution to find the average time each machine takes to complete a process.
# MAGIC
# MAGIC The time to complete a process is the 'end' timestamp minus the 'start' timestamp. The average time is calculated by the total time to complete every process on the machine divided by the number of processes that were run.
# MAGIC
# MAGIC The resulting table should have the machine_id along with the average time as processing_time, which should be rounded to 3 decimal places.
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
# MAGIC Activity table:
# MAGIC <pre>+------------+------------+---------------+-----------+
# MAGIC | machine_id | process_id | activity_type | timestamp |
# MAGIC +------------+------------+---------------+-----------+
# MAGIC | 0          | 0          | start         | 0.712     |
# MAGIC | 0          | 0          | end           | 1.520     |
# MAGIC | 0          | 1          | start         | 3.140     |
# MAGIC | 0          | 1          | end           | 4.120     |
# MAGIC | 1          | 0          | start         | 0.550     |
# MAGIC | 1          | 0          | end           | 1.550     |
# MAGIC | 1          | 1          | start         | 0.430     |
# MAGIC | 1          | 1          | end           | 1.420     |
# MAGIC | 2          | 0          | start         | 4.100     |
# MAGIC | 2          | 0          | end           | 4.512     |
# MAGIC | 2          | 1          | start         | 2.500     |
# MAGIC | 2          | 1          | end           | 5.000     |
# MAGIC +------------+------------+---------------+-----------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------------+-----------------+
# MAGIC | machine_id | processing_time |
# MAGIC +------------+-----------------+
# MAGIC | 0          | 0.894           |
# MAGIC | 1          | 0.995           |
# MAGIC | 2          | 1.456           |
# MAGIC +------------+-----------------+</pre>
# MAGIC Explanation: 
# MAGIC There are 3 machines running 2 processes each.
# MAGIC Machine 0's average time is ((1.520 - 0.712) + (4.120 - 3.140)) / 2 = 0.894
# MAGIC Machine 1's average time is ((1.550 - 0.550) + (1.420 - 0.430)) / 2 = 0.995
# MAGIC Machine 2's average time is ((4.512 - 4.100) + (5.000 - 2.500)) / 2 = 1.456

# COMMAND ----------

# Pandas Schema
import pandas as pd

data = [[0, 0, 'start', 0.712], [0, 0, 'end', 1.52], [0, 1, 'start', 3.14], [0, 1, 'end', 4.12], [1, 0, 'start', 0.55],
        [1, 0, 'end', 1.55], [1, 1, 'start', 0.43], [1, 1, 'end', 1.42], [2, 0, 'start', 4.1], [2, 0, 'end', 4.512],
        [2, 1, 'start', 2.5], [2, 1, 'end', 5]]
activity = pd.DataFrame(data, columns=['machine_id', 'process_id', 'activity_type', 'timestamp']).astype(
    {'machine_id': 'Int64', 'process_id': 'Int64', 'activity_type': 'object', 'timestamp': 'Float64'})

# COMMAND ----------

#to pyspark schema
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

activity_df = spark.createDataFrame(activity)
activity_df.show()

# COMMAND ----------

# In spark SQL first
import pyspark.sql.functions as F
### activity_df :  machine_id | process_id | activity_type | timestamp

# In SQL first
activity_df.createOrReplaceTempView("activity")
spark.sql("select a1.machine_id,sum((a2.timestamp-a1.timestamp))/count(distinct(a1.process_id)) \
          from activity a1 join activity a2 on a1.machine_id=a2.machine_id and a1.process_id=a2.process_id \
              and a1.activity_type='start' and a2.activity_type='end'\
                  group by a1.machine_id").show()
F.avg

# COMMAND ----------

### activity_df :  machine_id | process_id | activity_type | timestamp

## Notice == in every joining condition
## better to use expr in long joining conditions, which directly supports aliasing (so no col())
## notice logic - sum(col1-col2)/count(distinct(col3)) in aggragation

import pyspark.sql.functions as f
activity_df.alias("a1").join(activity_df.alias("a2"),f.expr("a1.machine_id==a2.machine_id and a1.process_id==a2.process_id and a1.activity_type=='start' and a2.activity_type=='end'"),how="inner") \
    .groupBy(f.col("a1.machine_id")) \
        .agg(f.sum(f.col("a2.timestamp")-f.col("a1.timestamp")) /f.countDistinct(f.col("a1.process_id")))\
            .show()

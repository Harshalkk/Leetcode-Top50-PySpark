# Databricks notebook source
# MAGIC %md
# MAGIC # [550. Game Play Analysis IV](https://leetcode.com/problems/game-play-analysis-iv/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Activity
# MAGIC
# MAGIC <pre>+--------------+---------+
# MAGIC | Column Name  | Type    |
# MAGIC +--------------+---------+
# MAGIC | player_id    | int     |
# MAGIC | device_id    | int     |
# MAGIC | event_date   | date    |
# MAGIC | games_played | int     |
# MAGIC +--------------+---------+</pre>
# MAGIC (player_id, event_date) is the primary key (combination of columns with unique values) of this table.
# MAGIC This table shows the activity of players of some games.
# MAGIC Each row is a record of a player who logged in and played a number of games (possibly 0) before logging out on someday using some device.
# MAGIC  
# MAGIC
# MAGIC Write a solution to report the fraction of players that logged in again on the day after the day they first logged in, rounded to 2 decimal places. In other words, you need to count the number of players that logged in for at least two consecutive days starting from their first login date, then divide that number by the total number of players.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Activity table:
# MAGIC <pre>+-----------+-----------+------------+--------------+
# MAGIC | player_id | device_id | event_date | games_played |
# MAGIC +-----------+-----------+------------+--------------+
# MAGIC | 1         | 2         | 2016-03-01 | 5            |
# MAGIC | 1         | 2         | 2016-03-02 | 6            |
# MAGIC | 2         | 3         | 2017-06-25 | 1            |
# MAGIC | 3         | 1         | 2016-03-02 | 0            |
# MAGIC | 3         | 4         | 2018-07-03 | 5            |
# MAGIC +-----------+-----------+------------+--------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+-----------+
# MAGIC | fraction  |
# MAGIC +-----------+
# MAGIC | 0.33      |
# MAGIC +-----------+</pre>
# MAGIC Explanation: 
# MAGIC Only the player with id 1 logged back in after the first day he had logged in so the answer is 1/3 = 0.33

# COMMAND ----------

#pandas schema
import pandas as pd

data = [[1, 2, '2016-03-01', 5], [1, 2, '2016-03-02', 6], [2, 3, '2017-06-25', 1], [3, 1, '2016-03-02', 0],
        [3, 4, '2018-07-03', 5]]
activity = pd.DataFrame(data, columns=['player_id', 'device_id', 'event_date', 'games_played']).astype(
    {'player_id': 'Int64', 'device_id': 'Int64', 'event_date': 'datetime64[ns]', 'games_played': 'Int64'})

# COMMAND ----------

# to spark dataframe

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

activity_df = spark.createDataFrame(activity)
activity_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
import pyspark.sql.functions as f
import pyspark.sql.window as w

# Input: Activity table:
# +-----------+-----------+------------+--------------+
# | player_id | device_id | event_date | games_played |
# +-----------+-----------+------------+--------------+
# | 1         | 2         | 2016-03-01 | 5            |

# Output:
# +-----------+
# | fraction  |

## Window specification uses w.Window without ()
## Actual window functions are in functions like f.row_number/lead/lag
## for value to be discarded in count just leave it, or put f.lit(None) in otherwise
## there is no then() after when, when takes 2nd param as value instead of seperate when

ws=w.Window.partitionBy('player_id').orderBy('event_date')

activity_df.alias('a').filter(f.col('games_played')>0).withColumn('next_date',f.lead('event_date',1).over(ws)) \
    .select( f.round(((f.count( \
                    f.when( (f.date_add(f.col('event_date'),1) == f.col('next_date')),1) \
                          ) \
              )/ f.countDistinct(f.col('player_id'))),2).alias('fraction')).show()

# COMMAND ----------

activity_df.createOrReplaceTempView('activity')

spark.sql('''
          with cte as (
              select player_id, lead(event_date,1,null) over(partition by player_id order by event_date) as next_date,event_date
              from activity where games_played>0
          )
          select  round(count(case when next_date=dateadd(event_date,1) then 1 end)/count(distinct (player_id)),2) as fraction
          from cte
        ''').show()
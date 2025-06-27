# Databricks notebook source
# MAGIC %md
# MAGIC # [197. Rising Temperature](https://leetcode.com/problems/rising-temperature/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Weather
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | id            | int     |
# MAGIC | recordDate    | date    |
# MAGIC | temperature   | int     |
# MAGIC +---------------+---------+</pre>
# MAGIC id is the column with unique values for this table.
# MAGIC This table contains information about the temperature on a certain day.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find all dates' Id with higher temperatures compared to its previous dates (yesterday).
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
# MAGIC Weather table:
# MAGIC <pre>+----+------------+-------------+
# MAGIC | id | recordDate | temperature |
# MAGIC +----+------------+-------------+
# MAGIC | 1  | 2015-01-01 | 10          |
# MAGIC | 2  | 2015-01-02 | 25          |
# MAGIC | 3  | 2015-01-03 | 20          |
# MAGIC | 4  | 2015-01-04 | 30          |
# MAGIC +----+------------+-------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+----+
# MAGIC | id |
# MAGIC +----+
# MAGIC | 2  |
# MAGIC | 4  |
# MAGIC +----+</pre>
# MAGIC Explanation: 
# MAGIC In 2015-01-02, the temperature was higher than the previous day (10 -> 25).
# MAGIC In 2015-01-04, the temperature was higher than the previous day (20 -> 30).

# COMMAND ----------

#Pandas schema

import pandas as pd

data = [[1, '2015-01-01', 10], [2, '2015-01-02', 25], [3, '2015-01-03', 20], [4, '2015-01-04', 30]]
weather = pd.DataFrame(data, columns=['id', 'recordDate', 'temperature']).astype({'id':'Int64', 'recordDate':'datetime64[ns]', 'temperature':'Int64'})

# COMMAND ----------

# to pyspark schema

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

weather_df = spark.createDataFrame(weather)
weather_df.show(truncate=False)

# COMMAND ----------

### weather_df
### | id | recordDate | temperature 

import pyspark.sql.window as w 
import pyspark.sql.functions as f


## Using window function
windowSpec=w.Window.orderBy("recordDate")

### Used lag function to Create new col with window function spec

weather_df.withColumn("previous_temp",f.lag("temperature",1,0).over(windowSpec)) \
    .filter(f.col("temperature")>f.col("previous_temp")) \
        .filter(f.col("previous_temp")!=0).show()


# COMMAND ----------

# in Spark SQL

## Using CTE for extracting previous day temp 
## window function can't be used in where condition
## also window function written in select as aliased column, can't be referred in where cluase
weather_df.createOrReplaceTempView("weather")
spark.sql("with cte as \
    (select id,lag(temperature,1,null) over(order by recordDate) previous_temp \
        from weather \
    ) \
    select w1.id\
          from weather w1 join cte c1 on c1.id=w1.id \
              where c1.previous_temp < w1.temperature").show()

# COMMAND ----------

spark.stop()
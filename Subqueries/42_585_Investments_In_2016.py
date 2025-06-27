# Databricks notebook source
# MAGIC %md
# MAGIC # [585. Investments in 2016](https://leetcode.com/problems/investments-in-2016/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Insurance
# MAGIC
# MAGIC <pre>+-------------+-------+
# MAGIC | Column Name | Type  |
# MAGIC +-------------+-------+
# MAGIC | pid         | int   |
# MAGIC | tiv_2015    | float |
# MAGIC | tiv_2016    | float |
# MAGIC | lat         | float |
# MAGIC | lon         | float |
# MAGIC +-------------+-------+</pre>
# MAGIC pid is the primary key (column with unique values) for this table.
# MAGIC Each row of this table contains information about one policy where:
# MAGIC pid is the policyholder's policy ID.
# MAGIC tiv_2015 is the total investment value in 2015 and tiv_2016 is the total investment value in 2016.
# MAGIC lat is the latitude of the policy holder's city. It's guaranteed that lat is not NULL.
# MAGIC lon is the longitude of the policy holder's city. It's guaranteed that lon is not NULL.
# MAGIC  
# MAGIC
# MAGIC Write a solution to report the sum of all total investment values in 2016 tiv_2016, for all policyholders who:
# MAGIC
# MAGIC have the same tiv_2015 value as one or more other policyholders, and
# MAGIC are not located in the same city as any other policyholder (i.e., the (lat, lon) attribute pairs must be unique).
# MAGIC Round tiv_2016 to two decimal places.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Insurance table:
# MAGIC <pre>+-----+----------+----------+-----+-----+
# MAGIC | pid | tiv_2015 | tiv_2016 | lat | lon |
# MAGIC +-----+----------+----------+-----+-----+
# MAGIC | 1   | 10       | 5        | 10  | 10  |
# MAGIC | 2   | 20       | 20       | 20  | 20  |
# MAGIC | 3   | 10       | 30       | 20  | 20  |
# MAGIC | 4   | 10       | 40       | 40  | 40  |
# MAGIC +-----+----------+----------+-----+-----+</pre>
# MAGIC Output: 
# MAGIC <pre>+----------+
# MAGIC | tiv_2016 |
# MAGIC +----------+
# MAGIC | 45.00    |
# MAGIC +----------+</pre>
# MAGIC Explanation: 
# MAGIC The first record in the table, like the last record, meets both of the two criteria.
# MAGIC The tiv_2015 value 10 is the same as the third and fourth records, and its location is unique.
# MAGIC
# MAGIC The second record does not meet any of the two criteria. Its tiv_2015 is not like any other policyholders and its location is the same as the third record, which makes the third record fail, too.
# MAGIC So, the result is the sum of tiv_2016 of the first and last record, which is 45.

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[1, 10, 5, 10, 10], [2, 20, 20, 20, 20], [3, 10, 30, 20, 20], [4, 10, 40, 40, 40]]
insurance = pd.DataFrame(data, columns=['pid', 'tiv_2015', 'tiv_2016', 'lat', 'lon']).astype(
    {'pid': 'Int64', 'tiv_2015': 'Float64', 'tiv_2016': 'Float64', 'lat': 'Float64', 'lon': 'Float64'})

# COMMAND ----------

# pandas df to pyspark df

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

insurance_df = spark.createDataFrame(insurance)
insurance_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f

# in Spark Dataframe
# Input: Insurance table:
# +-----+----------+----------+-----+-----+
# | pid | tiv_2015 | tiv_2016 | lat | lon |
# +-----+----------+----------+-----+-----+

## Logic :
# new cols created for ctr over location and policy
# filter cols created over this ctr columns to get true or false value
# when both are true - row is returned
## remember count(id) over() will return same value for each id, since it sees all rows for count unles rows between is given

insurance_df \
    .withColumn("pol_ctr",f.count('tiv_2015').over(Window.partitionBy("tiv_2015"))) \
    .withColumn("pol_filter",f.col("pol_ctr")>1) \
    .withColumn("loc_ctr",f.count(f.concat(f.col("lat"),f.col("lon"))).over(Window.partitionBy(f.concat(f.col("lat"),f.col("lon"))))) \
    .withColumn("loc_filter",f.col("loc_ctr")==1) \
    .filter((f.col("pol_filter")==True) & (f.col('loc_filter')==True)) \
    .agg(f.sum(f.col("tiv_2016")).alias("sum_amt")).show()
#     .withColumn("loc_ctr",f.row_number().over(Window.orderBy("pid").partitionBy(f.concat(f.col("lat"),f.col("lon"))))).show()

# COMMAND ----------

insurance_df.createOrReplaceTempView("insurance")
spark.sql(
    '''
    with cte as (
        select *
        ,count(tiv_2015) over(partition by tiv_2015) as pol_ctr
        ,count(concat(lat,lon)) over(partition by concat(lat,lon)) as loc_ctr
        from insurance
)
, cte2 as
(
    select *, case when pol_ctr>1 then true else false end as pol_filter
            , case when loc_ctr=1 then true else false end as loc_filter
    from cte
)
select sum(tiv_2016)
from cte2 where pol_filter and loc_filter
''').show()

# COMMAND ----------

spark.stop()
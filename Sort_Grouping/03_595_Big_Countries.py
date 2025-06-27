# Databricks notebook source
# MAGIC %md
# MAGIC # [595. Big Countries](https://leetcode.com/problems/big-countries/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: World
# MAGIC <pre>
# MAGIC +-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | name        | varchar |
# MAGIC | continent   | varchar |
# MAGIC | area        | int     |
# MAGIC | population  | int     |
# MAGIC | gdp         | bigint  |
# MAGIC +-------------+---------+
# MAGIC </pre>
# MAGIC name is the primary key (column with unique values) for this table.
# MAGIC Each row of this table gives information about the name of a country, the continent to which it belongs, its area, the population, and its GDP value.
# MAGIC  
# MAGIC
# MAGIC A country is big if:
# MAGIC
# MAGIC it has an area of at least three million (i.e., 3000000 km2), or
# MAGIC it has a population of at least twenty-five million (i.e., 25000000).
# MAGIC Write a solution to find the name, population, and area of the big countries.
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
# MAGIC World table:
# MAGIC <pre>
# MAGIC +-------------+-----------+---------+------------+--------------+
# MAGIC | name        | continent | area    | population | gdp          |
# MAGIC +-------------+-----------+---------+------------+--------------+
# MAGIC | Afghanistan | Asia      | 652230  | 25500100   | 20343000000  |
# MAGIC | Albania     | Europe    | 28748   | 2831741    | 12960000000  |
# MAGIC | Algeria     | Africa    | 2381741 | 37100000   | 188681000000 |
# MAGIC | Andorra     | Europe    | 468     | 78115      | 3712000000   |
# MAGIC | Angola      | Africa    | 1246700 | 20609294   | 100990000000 |
# MAGIC +-------------+-----------+---------+------------+--------------+
# MAGIC </pre>
# MAGIC Output: 
# MAGIC <pre>
# MAGIC +-------------+------------+---------+
# MAGIC | name        | population | area    |
# MAGIC +-------------+------------+---------+
# MAGIC | Afghanistan | 25500100   | 652230  |
# MAGIC | Algeria     | 37100000   | 2381741 |
# MAGIC +-------------+------------+---------+
# MAGIC </pre>

# COMMAND ----------

# Pandas Schema
import pandas as pd
# Afghanistan | Asia      | 652230  | 25500100   | 20343000000  |
# | Albania     | Europe    | 28748   | 2831741    | 12960000000  |
# | Algeria     | Africa    | 2381741 | 37100000   | 188681000000 |
# | Andorra     | Europe    | 468     | 78115      | 3712000000   |
# | Angola      | Africa    | 1246700 | 20609294   | 100990000000

# Creating normal array with data
data=[['Afghanistan','Asia',652230,25500100,20343000000],
      ['Albania',      'Europe', 28748   , 2831741    , 12960000000  ],
      [ 'Algeria',     'Africa'    , 2381741 , 37100000   , 188681000000 ],
      [ 'Andorra'     , 'Europe'    , 468     , 78115      , 3712000000   ],
      ['Angola',      'Africa'    , 1246700 , 20609294   , 100990000000]
]
# convertin array into spark df
## notice - data=data and schema with just cols array
sdf=spark.createDataFrame(data=data,schema=["name","continent", "area",    "population" ,"gdp"])
display(sdf)

# filtering with OR - |
sdf.filter((sdf['area']>3000000) | (sdf['population']>25000000)).show()

# COMMAND ----------

# In SQL
## notice df object to create view
sdf.createOrReplaceTempView("countries")

## spark.sql used to query that view
spark.sql("select * from countries where population >25000000 or area >3000000").show()

# COMMAND ----------

 spark.stop()
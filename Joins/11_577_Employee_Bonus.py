# Databricks notebook source
# MAGIC %md
# MAGIC # [577. Employee Bonus](https://leetcode.com/problems/employee-bonus/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Employee
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | empId       | int     |
# MAGIC | name        | varchar |
# MAGIC | supervisor  | int     |
# MAGIC | salary      | int     |
# MAGIC +-------------+---------+</pre>
# MAGIC empId is the column with unique values for this table.
# MAGIC Each row of this table indicates the name and the ID of an employee in addition to their salary and the id of their manager.
# MAGIC  
# MAGIC
# MAGIC Table: Bonus
# MAGIC
# MAGIC <pre>+-------------+------+
# MAGIC | Column Name | Type |
# MAGIC +-------------+------+
# MAGIC | empId       | int  |
# MAGIC | bonus       | int  |
# MAGIC +-------------+------+</pre>
# MAGIC empId is the column of unique values for this table.
# MAGIC empId is a foreign key (reference column) to empId from the Employee table.
# MAGIC Each row of this table contains the id of an employee and their respective bonus.
# MAGIC  
# MAGIC
# MAGIC Write a solution to report the name and bonus amount of each employee with a bonus less than 1000.
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
# MAGIC Employee table:
# MAGIC <pre>+-------+--------+------------+--------+
# MAGIC | empId | name   | supervisor | salary |
# MAGIC +-------+--------+------------+--------+
# MAGIC | 3     | Brad   | null       | 4000   |
# MAGIC | 1     | John   | 3          | 1000   |
# MAGIC | 2     | Dan    | 3          | 2000   |
# MAGIC | 4     | Thomas | 3          | 4000   |
# MAGIC +-------+--------+------------+--------+</pre>
# MAGIC Bonus table:
# MAGIC <pre>+-------+-------+
# MAGIC | empId | bonus |
# MAGIC +-------+-------+
# MAGIC | 2     | 500   |
# MAGIC | 4     | 2000  |
# MAGIC +-------+-------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------+-------+
# MAGIC | name | bonus |
# MAGIC +------+-------+
# MAGIC | Brad | null  |
# MAGIC | John | null  |
# MAGIC | Dan  | 500   |
# MAGIC +------+-------+</pre>

# COMMAND ----------

# Pandas Schema

import pandas as pd

data = [[3, 'Brad', None, 4000], [1, 'John', 3, 1000], [2, 'Dan', 3, 2000], [4, 'Thomas', 3, 4000]]
employee = pd.DataFrame(data, columns=['empId', 'name', 'supervisor', 'salary']).astype(
    {'empId': 'Int64', 'name': 'object', 'supervisor': 'Int64', 'salary': 'Int64'})
data2 = [[2, 500], [4, 2000]]
bonus = pd.DataFrame(data2, columns=['empId', 'bonus']).astype({'empId': 'Int64', 'bonus': 'Int64'})

# COMMAND ----------

# to pyspark schema

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

schema_employee = StructType([
    StructField("empId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("supervisor", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

employee_df = spark.createDataFrame(data, schema_employee)
bonus_df = spark.createDataFrame(bonus)

# COMMAND ----------

# In spark Dataframe
### employee_df

# | Column Name | Type    |
# +-------------+---------+
# | empId       | int     |
# | name        | varchar |
# | supervisor  | int     |
# | salary      | int    

### bonus_df
#  Column Name | Type |
# +-------------+------+
# | empId       | int  |
# | bonus       | int  |

### o/p

# +------+-------+
# | name | bonus |
# +------+-------+
# | Brad | null  |
# | John | null  |
# | Dan  | 500   |
# +------+-------+

import pyspark.sql.functions as f

## Notice we need round brackets for each condition compuslory in filter if there is more than one condition
employee_df.alias("e").join(bonus_df.alias("b"),f.col("e.empId")==f.col("b.empid"),how="left") \
    .filter((f.col("b.bonus")<1000) | (f.col("b.empId").isNull())) \
        .select(f.col("e.name"),f.col("b.bonus")).show()

# COMMAND ----------

# In spark SQL

employee_df.createOrReplaceTempView("employee")
bonus_df.createOrReplaceTempView("bonus")

spark.sql("select e.name,b.bonus \
    from employee e left join bonus b on e.empId=b.empId \
        where b.bonus<1000 or b.empId is null").show()

# COMMAND ----------

spark.stop()

# COMMAND ----------


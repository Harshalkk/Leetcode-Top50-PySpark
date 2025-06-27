# Databricks notebook source
# MAGIC %md
# MAGIC # [1789. Primary Department for Each Employee](https://leetcode.com/problems/primary-department-for-each-employee/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Employee
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   |  Type   |
# MAGIC +---------------+---------+
# MAGIC | employee_id   | int     |
# MAGIC | department_id | int     |
# MAGIC | primary_flag  | varchar |
# MAGIC +---------------+---------+</pre>
# MAGIC (employee_id, department_id) is the primary key (combination of columns with unique values) for this table.
# MAGIC employee_id is the id of the employee.
# MAGIC department_id is the id of the department to which the employee belongs.
# MAGIC primary_flag is an ENUM (category) of type ('Y', 'N'). If the flag is 'Y', the department is the primary department for the employee. If the flag is 'N', the department is not the primary.
# MAGIC  
# MAGIC
# MAGIC Employees can belong to multiple departments. When the employee joins other departments, they need to decide which department is their primary department. Note that when an employee belongs to only one department, their primary column is 'N'.
# MAGIC
# MAGIC Write a solution to report all the employees with their primary department. For employees who belong to one department, report their only department.
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
# MAGIC <pre>+-------------+---------------+--------------+
# MAGIC | employee_id | department_id | primary_flag |
# MAGIC +-------------+---------------+--------------+
# MAGIC | 1           | 1             | N            |
# MAGIC | 2           | 1             | Y            |
# MAGIC | 2           | 2             | N            |
# MAGIC | 3           | 3             | N            |
# MAGIC | 4           | 2             | N            |
# MAGIC | 4           | 3             | Y            |
# MAGIC | 4           | 4             | N            |
# MAGIC +-------------+---------------+--------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+-------------+---------------+
# MAGIC | employee_id | department_id |
# MAGIC +-------------+---------------+
# MAGIC | 1           | 1             |
# MAGIC | 2           | 1             |
# MAGIC | 3           | 3             |
# MAGIC | 4           | 3             |
# MAGIC +-------------+---------------+</pre>
# MAGIC Explanation: 
# MAGIC - The Primary department for employee 1 is 1.
# MAGIC - The Primary department for employee 2 is 1.
# MAGIC - The Primary department for employee 3 is 3.
# MAGIC - The Primary department for employee 4 is 3.

# COMMAND ----------

# pandas schema

import pandas as pd

data = [['1', '1', 'N'], ['2', '1', 'Y'], ['2', '2', 'N'], ['3', '3', 'N'], ['4', '2', 'N'], ['4', '3', 'Y'],
        ['4', '4', 'N']]
employee = pd.DataFrame(data, columns=['employee_id', 'department_id', 'primary_flag']).astype(
    {'employee_id': 'Int64', 'department_id': 'Int64', 'primary_flag': 'object'})

# COMMAND ----------

# to pyspark schema
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

employee_df = spark.createDataFrame(employee)
employee_df.show()

# COMMAND ----------

import pyspark.sql.functions as f
# Input: Employee table:
# +-------------+---------------+--------------+
# | employee_id | department_id | primary_flag |
# +-------------+---------------+--------------+
# | 1           | 1             | N            |

# Output:
# +-------------+---------------+
# | employee_id | dep

## f.count(ColName) is used as window function
## In filter, either primary_flag=1 or total_count (derived from window functions) of employee = 1
import pyspark.sql.window as w 
employee_df.withColumn("cc" \
    ,f.count('employee_id').over(w.Window.partitionBy('employee_id'))) \
        .filter(( f.col('primary_flag')=='Y') | (f.col('cc')==1 )) \
        .select(f.col("employee_id"),f.col("department_id")).show()


# COMMAND ----------

# In spark SQL

employee_df.createOrReplaceTempView("Employee")
spark.sql('''
          with cte as (
              select *, count(employee_id) over(partition by employee_id) as cc
              from Employee
          )
          select employee_id, department_id
          from cte
          where primary_flag = 'Y' OR cc = 1
          ''').show()

# COMMAND ----------


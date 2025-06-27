# Databricks notebook source
# MAGIC %md
# MAGIC # [1075. Project Employees I](https://leetcode.com/problems/project-employees-i/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Project
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | project_id  | int     |
# MAGIC | employee_id | int     |
# MAGIC +-------------+---------+</pre>
# MAGIC (project_id, employee_id) is the primary key of this table.
# MAGIC employee_id is a foreign key to Employee table.
# MAGIC Each row of this table indicates that the employee with employee_id is working on the project with project_id.
# MAGIC  
# MAGIC
# MAGIC Table: Employee
# MAGIC
# MAGIC <pre>+------------------+---------+
# MAGIC | Column Name      | Type    |
# MAGIC +------------------+---------+
# MAGIC | employee_id      | int     |
# MAGIC | name             | varchar |
# MAGIC | experience_years | int     |
# MAGIC +------------------+---------+</pre>
# MAGIC employee_id is the primary key of this table. It's guaranteed that experience_years is not NULL.
# MAGIC Each row of this table contains information about one employee.
# MAGIC  
# MAGIC
# MAGIC Write an SQL query that reports the average experience years of all the employees for each project, rounded to 2 digits.
# MAGIC
# MAGIC Return the result table in any order.
# MAGIC
# MAGIC The query result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Project table:
# MAGIC <pre>+-------------+-------------+
# MAGIC | project_id  | employee_id |
# MAGIC +-------------+-------------+
# MAGIC | 1           | 1           |
# MAGIC | 1           | 2           |
# MAGIC | 1           | 3           |
# MAGIC | 2           | 1           |
# MAGIC | 2           | 4           |
# MAGIC +-------------+-------------+</pre>
# MAGIC Employee table:
# MAGIC <pre>+-------------+--------+------------------+
# MAGIC | employee_id | name   | experience_years |
# MAGIC +-------------+--------+------------------+
# MAGIC | 1           | Khaled | 3                |
# MAGIC | 2           | Ali    | 2                |
# MAGIC | 3           | John   | 1                |
# MAGIC | 4           | Doe    | 2                |
# MAGIC +-------------+--------+------------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+-------------+---------------+
# MAGIC | project_id  | average_years |
# MAGIC +-------------+---------------+
# MAGIC | 1           | 2.00          |
# MAGIC | 2           | 2.50          |
# MAGIC +-------------+---------------+</pre>
# MAGIC Explanation: The average experience years for the first project is (3 + 2 + 1) / 3 = 2.00 and for the second project is (3 + 2) / 2 = 2.50

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[1, 1], [1, 2], [1, 3], [2, 1], [2, 4]]
project = pd.DataFrame(data, columns=['project_id', 'employee_id']).astype(
    {'project_id': 'Int64', 'employee_id': 'Int64'})
data = [[1, 'Khaled', 3], [2, 'Ali', 2], [3, 'John', 1], [4, 'Doe', 2]]
employee = pd.DataFrame(data, columns=['employee_id', 'name', 'experience_years']).astype(
    {'employee_id': 'Int64', 'name': 'object', 'experience_years': 'Int64'})

# COMMAND ----------

# to pyspark dataframe

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

project_df = spark.createDataFrame(project)
project_df.show(truncate=False)

# COMMAND ----------

employee_df = spark.createDataFrame(employee)
employee_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
# Project table:
# +-------------+-------------+
# | project_id  | employee_id |
# +-------------+-------------+
# | 1           | 1           |
# | 1           | 2           |

# Employee table:
# +-------------+--------+------------------+
# | employee_id | name   | experience_years |
# +-------------+--------+------------------+
# | 1           | Khaled | 3                |
# | 2           | Ali    | 2                |

# o/ p project_id  | average_years |
import pyspark.sql.functions as f
project_df.alias('p').join(employee_df.alias('e'),on="employee_id",how='left') \
    .groupBy(f.col('p.project_id')) \
        .agg((f.sum('e.experience_years')/f.count(f.col('p.employee_id'))).alias('average_years')) \
            .show()


# COMMAND ----------

# solving in spark sql

employee_df.createOrReplaceTempView("Employee")
project_df.createOrReplaceTempView("Project")

spark.sql('''
          select p.project_id, sum(e.experience_years)/count(p.employee_id) as average_experience
          from Employee e right join Project p
            on p.employee_id = e.employee_id
          group by p.project_id
          ''').show()
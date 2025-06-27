# Databricks notebook source
# MAGIC %md
# MAGIC # [176. Second Highest Salary](https://leetcode.com/problems/second-highest-salary/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Employee
# MAGIC
# MAGIC <pre>+-------------+------+
# MAGIC | Column Name | Type |
# MAGIC +-------------+------+
# MAGIC | id          | int  |
# MAGIC | salary      | int  |
# MAGIC +-------------+------+</pre>
# MAGIC id is the primary key (column with unique values) for this table.
# MAGIC Each row of this table contains information about the salary of an employee.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the second highest salary from the Employee table. If there is no second highest salary, return null (return None in Pandas).
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Employee table:
# MAGIC <pre>+----+--------+
# MAGIC | id | salary |
# MAGIC +----+--------+
# MAGIC | 1  | 100    |
# MAGIC | 2  | 200    |
# MAGIC | 3  | 300    |
# MAGIC +----+--------+</pre>
# MAGIC Output: 
# MAGIC <pre>+---------------------+
# MAGIC | SecondHighestSalary |
# MAGIC +---------------------+
# MAGIC | 200                 |
# MAGIC +---------------------+</pre>
# MAGIC Example 2:
# MAGIC
# MAGIC Input: 
# MAGIC Employee table:
# MAGIC <pre>+----+--------+
# MAGIC | id | salary |
# MAGIC +----+--------+
# MAGIC | 1  | 100    |
# MAGIC +----+--------+</pre>
# MAGIC Output: 
# MAGIC <pre>+---------------------+
# MAGIC | SecondHighestSalary |
# MAGIC +---------------------+
# MAGIC | null                |
# MAGIC +---------------------+</pre>

# COMMAND ----------

# pandas schema

import pandas as pd

# data = [[1, 100], [2, 200], [3, 300]] # example 1
data = [[1, 100]]  # example 2
employee = pd.DataFrame(data, columns=['id', 'salary']).astype({'id': 'int64', 'salary': 'int64'})

# COMMAND ----------

# to spark dataframe

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

employee_df = spark.createDataFrame(employee)
employee_df.show(truncate=False)

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.window as w
import pyspark.sql.types as t

# How to create df and union it inline 

employee_df.select("salary").unionAll(spark.createDataFrame([f.lit(None)], \
    schema=t.StructType([t.StructField("salary",t.StringType())]))) \
.withColumn("sal_rank",f.dense_rank().over(w.Window.orderBy(f.desc(f.col("salary"))))) \
    .filter(f.col("sal_rank")==2) \
    .select("salary").alias("2ndTopSalary").show()

# COMMAND ----------

# solving in spark SQL

employee_df.createOrReplaceTempView("employee")

spark.sql(
    '''
    select salary as 2ndTopSal
    from (
    select salary,dense_rank() over(order by salary desc) as rn
    from(
        select distinct salary from employee
        UNION all
        select null
    )

    ) where rn=2

    '''
    ).show()

# COMMAND ----------

spark.stop()
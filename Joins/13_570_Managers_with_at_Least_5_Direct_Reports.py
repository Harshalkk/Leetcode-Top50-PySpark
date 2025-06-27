# Databricks notebook source
# MAGIC %md
# MAGIC # [570. Managers with at Least 5 Direct Reports](https://leetcode.com/problems/managers-with-at-least-5-direct-reports/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Employee
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | id          | int     |
# MAGIC | name        | varchar |
# MAGIC | department  | varchar |
# MAGIC | managerId   | int     |
# MAGIC +-------------+---------+</pre>
# MAGIC id is the primary key (column with unique values) for this table.
# MAGIC Each row of this table indicates the name of an employee, their department, and the id of their manager.
# MAGIC If managerId is null, then the employee does not have a manager.
# MAGIC No employee will be the manager of themself.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find managers with at least five direct reports.
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
# MAGIC <pre>+-----+-------+------------+-----------+
# MAGIC | id  | name  | department | managerId |
# MAGIC +-----+-------+------------+-----------+
# MAGIC | 101 | John  | A          | None      |
# MAGIC | 102 | Dan   | A          | 101       |
# MAGIC | 103 | James | A          | 101       |
# MAGIC | 104 | Amy   | A          | 101       |
# MAGIC | 105 | Anne  | A          | 101       |
# MAGIC | 106 | Ron   | B          | 101       |
# MAGIC +-----+-------+------------+-----------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------+
# MAGIC | name |
# MAGIC +------+
# MAGIC | John |
# MAGIC +------+</pre>

# COMMAND ----------

# Pandas schema

import pandas as pd

data = [[101, 'John', 'A', None], [102, 'Dan', 'A', 101], [103, 'James', 'A', 101], [104, 'Amy', 'A', 101], [105, 'Anne', 'A', 101], [106, 'Ron', 'B', 101]]
employee = pd.DataFrame(data, columns=['id', 'name', 'department', 'managerId']).astype({'id':'Int64', 'name':'object', 'department':'object', 'managerId':'Int64'})

# COMMAND ----------

# to pyspark schema

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("managerId", IntegerType(), True)
])

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

employee_df = spark.createDataFrame(data,schema=schema)
employee_df.show(truncate=False)

# COMMAND ----------

# in Spark Dataframe

### : Employee table:

# +-----+-------+------------+-----------+
# | id  | name  | department | managerId |
# +-----+-------+------------+-----------+

# At least 5 subordinates
import pyspark.sql.functions as f

# method 1 
### notice we are making new df having only managerIds satisfying the critera & then joining it existing df to get their names
managerIds=employee_df.alias("e1").groupBy(f.col("e1.managerId")).agg(f.count("e1.*").alias("no_of_subs")).filter(f.col("no_of_subs")>=5).select(f.col("e1.managerId").alias("id"))
employee_df.alias("e1").join(managerIds,on="id",how="inner").select("e1.name").show()

# method 2 , Please check in oginal sol, not understanding it - https://github.com/kailasneupane/LeetCode-SQL-PySpark-Solutions/blob/main/studyplan-top-sql-50/2_Basic_Joins/13_570_Managers_with_at_Least_5_Direct_Reports.ipynb
# managerIds=employee_df.alias("e1").groupBy(f.col("e1.managerId")).agg(f.count("e1.*").alias("no_of_subs")).filter(f.col("no_of_subs")>=5).select(f.col("e1.managerId").alias("id")).collect()
# employee_df.alias("e1").filter(f.col("e1.id").isin(list(managerIds.select(f.col("id"))))).select("e1.name").show()


# COMMAND ----------

# in Spark SQL

employee_df.createOrReplaceTempView("employee")
spark.sql('''
          select e1.name
          from employee e1
          where id in (select e2.managerId from employee e2 
                                    group by e2.managerId 
                                    having count(e2.managerId)>=5
                        )
        ''').show()

# COMMAND ----------

spark.stop()

# COMMAND ----------


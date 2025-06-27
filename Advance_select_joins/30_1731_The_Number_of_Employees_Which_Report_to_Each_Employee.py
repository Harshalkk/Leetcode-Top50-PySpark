# Databricks notebook source
# MAGIC %md
# MAGIC # [1731. The Number of Employees Which Report to Each Employee](https://leetcode.com/problems/the-number-of-employees-which-report-to-each-employee/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Employees
# MAGIC
# MAGIC <pre>+-------------+----------+
# MAGIC | Column Name | Type     |
# MAGIC +-------------+----------+
# MAGIC | employee_id | int      |
# MAGIC | name        | varchar  |
# MAGIC | reports_to  | int      |
# MAGIC | age         | int      |
# MAGIC +-------------+----------+</pre>
# MAGIC employee_id is the column with unique values for this table.
# MAGIC This table contains information about the employees and the id of the manager they report to. Some employees do not report to anyone (reports_to is null). 
# MAGIC  
# MAGIC
# MAGIC For this problem, we will consider a manager an employee who has at least 1 other employee reporting to them.
# MAGIC
# MAGIC Write a solution to report the ids and the names of all managers, the number of employees who report directly to them, and the average age of the reports rounded to the nearest integer.
# MAGIC
# MAGIC Return the result table ordered by employee_id.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Employees table:
# MAGIC <pre>+-------------+---------+------------+-----+
# MAGIC | employee_id | name    | reports_to | age |
# MAGIC +-------------+---------+------------+-----+
# MAGIC | 9           | Hercy   | null       | 43  |
# MAGIC | 6           | Alice   | 9          | 41  |
# MAGIC | 4           | Bob     | 9          | 36  |
# MAGIC | 2           | Winston | null       | 37  |
# MAGIC +-------------+---------+------------+-----+</pre>
# MAGIC Output: 
# MAGIC <pre>+-------------+-------+---------------+-------------+
# MAGIC | employee_id | name  | reports_count | average_age |
# MAGIC +-------------+-------+---------------+-------------+
# MAGIC | 9           | Hercy | 2             | 39          |
# MAGIC +-------------+-------+---------------+-------------+</pre>
# MAGIC Explanation: Hercy has 2 people report directly to him, Alice and Bob. Their average age is (41+36)/2 = 38.5, which is 39 after rounding it to the nearest integer.

# COMMAND ----------

# Pandas schema

import pandas as pd

data = [[9, 'Hercy', None, 43], [6, 'Alice', 9, 41], [4, 'Bob', 9, 36], [2, 'Winston', None, 37]]
employees = pd.DataFrame(data, columns=['employee_id', 'name', 'reports_to', 'age']) \
    .astype({'employee_id': 'Int64', 'name': 'object', 'reports_to': 'Int64', 'age': 'Int64'})

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# To pyspark schema

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("reports_to", IntegerType(), True),
    StructField("age", IntegerType(), True)
])

employees_df = spark.createDataFrame(data, schema=schema)
employees_df.show()

# COMMAND ----------

# In spark dataframe
from pyspark.sql import functions as f
# Input: Employees table:
# +-------------+---------+------------+-----+
# | employee_id | name    | reports_to | age |

# Output:
# +-------------+-------+---------------+-------------+
# | employee_id | name  | reports_count | average_age |


## self joins always when columns from same tables gets involved 
employees_df.alias('e1').join(employees_df.alias('e2'),f.col('e1.employee_id')==f.col("e2.reports_to")) \
    .groupBy("e1.employee_id") \
        .agg(f.count("e2.employee_id").alias("no_of_reportee"), \
            f.round(f.avg(f.col("e2.age")),0).alias("avg_age")).show()

# COMMAND ----------

employees_df.createOrReplaceTempView("employee")

spark.sql(
    '''
    select e1.employee_id, count(e2.employee_id) as no_of_reportee, round(avg(e2.age),0) as average_age
    from employee e1
    join employee e2 on e1.employee_id = e2.reports_to
    group by e1.employee_id
    '''
).show()

# COMMAND ----------

spark.stop()

# COMMAND ----------


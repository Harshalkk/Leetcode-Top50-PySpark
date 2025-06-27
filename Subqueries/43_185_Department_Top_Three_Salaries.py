# Databricks notebook source
# MAGIC %md
# MAGIC # [185. Department Top Three Salaries](https://leetcode.com/problems/department-top-three-salaries/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Employee
# MAGIC
# MAGIC <pre>+--------------+---------+
# MAGIC | Column Name  | Type    |
# MAGIC +--------------+---------+
# MAGIC | id           | int     |
# MAGIC | name         | varchar |
# MAGIC | salary       | int     |
# MAGIC | departmentId | int     |
# MAGIC +--------------+---------+</pre>
# MAGIC id is the primary key (column with unique values) for this table.
# MAGIC departmentId is a foreign key (reference column) of the ID from the Department table.
# MAGIC Each row of this table indicates the ID, name, and salary of an employee. It also contains the ID of their department.
# MAGIC  
# MAGIC
# MAGIC Table: Department
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | id          | int     |
# MAGIC | name        | varchar |
# MAGIC +-------------+---------+</pre>
# MAGIC id is the primary key (column with unique values) for this table.
# MAGIC Each row of this table indicates the ID of a department and its name.
# MAGIC  
# MAGIC
# MAGIC A company's executives are interested in seeing who earns the most money in each of the company's departments. A high earner in a department is an employee who has a salary in the top three unique salaries for that department.
# MAGIC
# MAGIC Write a solution to find the employees who are high earners in each of the departments.
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
# MAGIC <pre>+----+-------+--------+--------------+
# MAGIC | id | name  | salary | departmentId |
# MAGIC +----+-------+--------+--------------+
# MAGIC | 1  | Joe   | 85000  | 1            |
# MAGIC | 2  | Henry | 80000  | 2            |
# MAGIC | 3  | Sam   | 60000  | 2            |
# MAGIC | 4  | Max   | 90000  | 1            |
# MAGIC | 5  | Janet | 69000  | 1            |
# MAGIC | 6  | Randy | 85000  | 1            |
# MAGIC | 7  | Will  | 70000  | 1            |
# MAGIC +----+-------+--------+--------------+</pre>
# MAGIC Department table:
# MAGIC <pre>+----+-------+
# MAGIC | id | name  |
# MAGIC +----+-------+
# MAGIC | 1  | IT    |
# MAGIC | 2  | Sales |
# MAGIC +----+-------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------------+----------+--------+
# MAGIC | Department | Employee | Salary |
# MAGIC +------------+----------+--------+
# MAGIC | IT         | Max      | 90000  |
# MAGIC | IT         | Joe      | 85000  |
# MAGIC | IT         | Randy    | 85000  |
# MAGIC | IT         | Will     | 70000  |
# MAGIC | Sales      | Henry    | 80000  |
# MAGIC | Sales      | Sam      | 60000  |
# MAGIC +------------+----------+--------+</pre>
# MAGIC Explanation: 
# MAGIC In the IT department:
# MAGIC - Max earns the highest unique salary
# MAGIC - Both Randy and Joe earn the second-highest unique salary
# MAGIC - Will earns the third-highest unique salary
# MAGIC
# MAGIC In the Sales department:
# MAGIC - Henry earns the highest salary
# MAGIC - Sam earns the second-highest salary
# MAGIC - There is no third-highest salary as there are only two employees

# COMMAND ----------

#Pandas schema

import pandas as pd

data = [[1, 'Joe', 85000, 1], [2, 'Henry', 80000, 2], [3, 'Sam', 60000, 2], [4, 'Max', 90000, 1],
        [5, 'Janet', 69000, 1], [6, 'Randy', 85000, 1], [7, 'Will', 70000, 1]]
employee = pd.DataFrame(data, columns=['id', 'name', 'salary', 'departmentId']).astype(
    {'id': 'Int64', 'name': 'object', 'salary': 'Int64', 'departmentId': 'Int64'})
data = [[1, 'IT'], [2, 'Sales']]
department = pd.DataFrame(data, columns=['id', 'name']).astype({'id': 'Int64', 'name': 'object'})

# COMMAND ----------

# pandas df to spark df

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

employee_df = spark.createDataFrame(employee)
employee_df.show(truncate=False)

# COMMAND ----------

department_df = spark.createDataFrame(department)
department_df.show(truncate=False)

# COMMAND ----------

# Input: Employee table:
# +----+-------+--------+--------------+
# | id | name  | salary | departmentId |
# +----+-------+--------+--------------+
# Department table:
# +----+-------+
# | id | name  |
# +----+-------+

# Output:
# +------------+----------+--------+
# | Department | Employee | Salary |
# +------------+----------+--------+
# | IT         | Max      | 90000  |

import pyspark.sql.functions as f
import pyspark.sql.window as w
employee_df.alias('e').join(department_df.alias('d'),f.col('departmentId')==f.col("d.id")) \
   .withColumn("sal_rank",f.dense_rank().over(w.Window.partitionBy(f.col("e.departmentId")).orderBy(f.desc(f.col("e.salary"))))) \
       .filter(f.col("sal_rank")<=3) \
       .select(f.col("d.name").alias("department"),f.col("e.name"),f.col("e.salary")) \
           .orderBy(["department",f.desc("salary")]) \
           .show()


# COMMAND ----------

# in Spark SQL
employee_df.createOrReplaceTempView("employee")
department_df.createOrReplaceTempView("department")

spark.sql(
    '''
    with cte as(
        select d.name as department_name,e.name as employee_name,e.salary, 
            dense_rank() over (partition by departmentId order by salary desc) as sal_rank 
        from employee e
            join department d on d.id=departmentId
    )
    select  department_name,employee_name,salary
    from cte
    where sal_rank<=3
    order by 1,3 desc
    '''
).show()

# COMMAND ----------


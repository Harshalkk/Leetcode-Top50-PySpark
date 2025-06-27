# Databricks notebook source
# MAGIC %md
# MAGIC # [1978. Employees Whose Manager Left the Company](https://leetcode.com/problems/employees-whose-manager-left-the-company/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Employees
# MAGIC
# MAGIC <pre>+-------------+----------+
# MAGIC | Column Name | Type     |
# MAGIC +-------------+----------+
# MAGIC | employee_id | int      |
# MAGIC | name        | varchar  |
# MAGIC | manager_id  | int      |
# MAGIC | salary      | int      |
# MAGIC +-------------+----------+</pre>
# MAGIC In SQL, employee_id is the primary key for this table.
# MAGIC This table contains information about the employees, their salary, and the ID of their manager. Some employees do not have a manager (manager_id is null). 
# MAGIC  
# MAGIC
# MAGIC Find the IDs of the employees whose salary is strictly less than dollar 30000 and whose manager left the company. When a manager leaves the company, their information is deleted from the Employees table, but the reports still have their manager_id set to the manager that left.
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
# MAGIC <pre>+-------------+-----------+------------+--------+
# MAGIC | employee_id | name      | manager_id | salary |
# MAGIC +-------------+-----------+------------+--------+
# MAGIC | 3           | Mila      | 9          | 60301  |
# MAGIC | 12          | Antonella | null       | 31000  |
# MAGIC | 13          | Emery     | null       | 67084  |
# MAGIC | 1           | Kalel     | 11         | 21241  |
# MAGIC | 9           | Mikaela   | null       | 50937  |
# MAGIC | 11          | Joziah    | 6          | 28485  |
# MAGIC +-------------+-----------+------------+--------+</pre>
# MAGIC Output: 
# MAGIC <pre>+-------------+
# MAGIC | employee_id |
# MAGIC +-------------+
# MAGIC | 11          |
# MAGIC +-------------+</pre>
# MAGIC
# MAGIC Explanation: 
# MAGIC The employees with a salary less than $30000 are 1 (Kalel) and 11 (Joziah).
# MAGIC Kalel's manager is employee 11, who is still in the company (Joziah).
# MAGIC Joziah's manager is employee 6, who left the company because there is no row for employee 6 as it was deleted.

# COMMAND ----------

#pandas schema
import pandas as pd

data = [[3, 'Mila', 9, 60301], [12, 'Antonella', None, 31000], [13, 'Emery', None, 67084], [1, 'Kalel', 11, 21241],
        [9, 'Mikaela', None, 50937], [11, 'Joziah', 6, 28485]]
employees = pd.DataFrame(data, columns=['employee_id', 'name', 'manager_id', 'salary']).astype(
    {'employee_id': 'Int64', 'name': 'object', 'manager_id': 'Int64', 'salary': 'Int64'})

# spark has issues with null in Int64 of pandas, hence converting to str for now. Will convert to int after loading in spark.
employees['manager_id'] = employees['manager_id'].astype('str')

# COMMAND ----------

employees

# COMMAND ----------

#pyspark schema

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

employees_df = spark.createDataFrame(employees)
employees_df.withColumn('manager_id', F.col('manager_id').cast('int')).show()

# COMMAND ----------

employees_df.printSchema()

# COMMAND ----------

# Solving in pyspark dataframe


## anti join used 
import pyspark.sql.functions as f
employees_df.alias('e1').join(employees_df.alias('e2'),f.col('e1.employee_id')==f.col("e2.manager_id"),how="anti") \
    .filter(f.col('e1.salary')<30000) \
        .select("employee_id").show()


# employee_ids=employees_df.select('employee_id').collect()
# employees_df.filter((f.col('salary')<3000) & (~f.col("manage_id").isin(f.collect_list(employee_ids)))) \
#     .select('employee_id') \
#     .show()

# COMMAND ----------

# another way

## why do we need rdd map for creating list?
employee_list = employees_df.select('employee_id').rdd.map(lambda x: x[0]).collect()

employees_df \
    .filter((F.col('salary') < 30000) & (~F.col('manager_id').isin(employee_list))) \
    .select('employee_id') \
    .orderBy('employee_id') \
    .show()

# COMMAND ----------

# In Spark SQL

employees_df.createOrReplaceTempView('employees')

spark.sql(
    '''
    select employee_id
    from employees e1 anti join employees e2 on e1.employee_id = e2.manager_id
    where e1.salary<30000
    '''
).show()
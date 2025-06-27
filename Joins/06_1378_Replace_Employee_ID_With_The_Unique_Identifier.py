# Databricks notebook source
# MAGIC %md
# MAGIC # [1378. Replace Employee ID With The Unique Identifier](https://leetcode.com/problems/replace-employee-id-with-the-unique-identifier/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Employees
# MAGIC <pre>
# MAGIC +---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | id            | int     |
# MAGIC | name          | varchar |
# MAGIC +---------------+---------+</pre>
# MAGIC id is the primary key (column with unique values) for this table.
# MAGIC Each row of this table contains the id and the name of an employee in a company.
# MAGIC  
# MAGIC
# MAGIC Table: EmployeeUNI
# MAGIC <pre>
# MAGIC +---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | id            | int     |
# MAGIC | unique_id     | int     |
# MAGIC +---------------+---------+</pre>
# MAGIC (id, unique_id) is the primary key (combination of columns with unique values) for this table.
# MAGIC Each row of this table contains the id and the corresponding unique id of an employee in the company.
# MAGIC  
# MAGIC
# MAGIC Write a solution to show the unique ID of each user, If a user does not have a unique ID replace just show null.
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
# MAGIC Employees table:
# MAGIC <pre>+----+----------+
# MAGIC | id | name     |
# MAGIC +----+----------+
# MAGIC | 1  | Alice    |
# MAGIC | 7  | Bob      |
# MAGIC | 11 | Meir     |
# MAGIC | 90 | Winston  |
# MAGIC | 3  | Jonathan |
# MAGIC +----+----------+</pre>
# MAGIC EmployeeUNI table:
# MAGIC <pre>+----+-----------+
# MAGIC | id | unique_id |
# MAGIC +----+-----------+
# MAGIC | 3  | 1         |
# MAGIC | 11 | 2         |
# MAGIC | 90 | 3         |
# MAGIC +----+-----------+</pre>
# MAGIC Output: 
# MAGIC <pre>+-----------+----------+
# MAGIC | unique_id | name     |
# MAGIC +-----------+----------+
# MAGIC | null      | Alice    |
# MAGIC | null      | Bob      |
# MAGIC | 2         | Meir     |
# MAGIC | 3         | Winston  |
# MAGIC | 1         | Jonathan |
# MAGIC +-----------+----------+</pre>
# MAGIC Explanation: 
# MAGIC Alice and Bob do not have a unique ID, We will show null instead.
# MAGIC The unique ID of Meir is 2.
# MAGIC The unique ID of Winston is 3.
# MAGIC The unique ID of Jonathan is 1.

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as f

# Creating panda & spark df for 1st table
data=[
    [ 1  ,'Alice'],
[7  ,'Bob'      ],
[11, 'Meir'],
[90,'Winston'],
[3,'Jonathan'],
]
pdf=pd.DataFrame(data=data,columns=['id','name'])
pdf.astype({'id':int,'name':object})

#display(pdf)
#pdf.info()

sdfEmp=spark.createDataFrame(data=pdf)
#display(sdfEmp)

# creating panda & spark df for 2nd table
data=[
    [ 3  ,1],
[11  ,2],
[90,3],
]
pdf=pd.DataFrame(data=data,columns=['id','unique_id'])
pdf.astype({'id':int,'unique_id':int})

#display(pdf)
#pdf.info()

sdfEmpU=spark.createDataFrame(data=pdf)
display(sdfEmpU)


# joining both tables
## on can be only used when column names are same and can pass array like ['id','key']
# sdfEmp.alias('e1').join(sdfEmpU.alias('e2'),how="left",on="id").show()

## for diff column names in both tables, give condition as 2nd position argument
## notice if we want to use alias, it should be in col()
## if without alias, give it as sdfEmp.e1
## Round brackets () are compulsory in condition withoit on
## == needs to be given instead of =
sdfEmp.alias('e1').join(sdfEmpU.alias('e2'),(f.col("e1.id")==f.col("e2.id")),how="left").show()

# COMMAND ----------

# In Sql
## Need to create view for each df seperately
sdfEmp.createOrReplaceTempView("Emp")
sdfEmpU.createOrReplaceTempView("EmpU")
spark.sql("select e1.*,e2.unique_id from Emp e1 left join EmpU e2 on e1.id=e2.id").show()
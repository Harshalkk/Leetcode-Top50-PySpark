# Databricks notebook source
# MAGIC %md
# MAGIC # [584. Find Customer Referee](https://leetcode.com/problems/find-customer-referee/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Customer
# MAGIC
# MAGIC <pre>
# MAGIC +-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | id          | int     |
# MAGIC | name        | varchar |
# MAGIC | referee_id  | int     |
# MAGIC +-------------+---------+</pre>
# MAGIC In SQL, id is the primary key column for this table.
# MAGIC Each row of this table indicates the id of a customer, their name, and the id of the customer who referred them.
# MAGIC  
# MAGIC
# MAGIC Find the names of the customer that are not referred by the customer with id = 2.
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
# MAGIC Customer table:
# MAGIC <pre>
# MAGIC +----+------+------------+
# MAGIC | id | name | referee_id |
# MAGIC +----+------+------------+
# MAGIC | 1  | Will | null       |
# MAGIC | 2  | Jane | null       |
# MAGIC | 3  | Alex | 2          |
# MAGIC | 4  | Bill | null       |
# MAGIC | 5  | Zack | 1          |
# MAGIC | 6  | Mark | 2          |
# MAGIC +----+------+------------+
# MAGIC </pre>
# MAGIC Output:
# MAGIC <pre> 
# MAGIC +------+
# MAGIC | name |
# MAGIC +------+
# MAGIC | Will |
# MAGIC | Jane |
# MAGIC | Bill |
# MAGIC | Zack |
# MAGIC +------+
# MAGIC </pre>

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql.types import *

# create data from numpy
data=np.array([[1,'Will',None],
[2,  'Jane' , None  ] ,
[3 , 'Alex', 2],
[4,  'Bill',None], 
[5  ,'Zack', 1],
[6,'Mark',2]] )  

# create panda df with columns and their type
## notice astype taking dict of 'col_name':'TypeName'
pdf=pd.DataFrame(data,columns=['id','name','referee_id']).astype({'id':'Int64','name':'object','referee_id':'Int64'})

# create schema for spark df(maybe not req if we have dtypes in panda defined above)
schema=StructType([
    StructField('id',IntegerType()),
    StructField('Name',StringType()),
    StructField('referee_id',IntegerType())
]
)

# create spark df from panda df
df=spark.createDataFrame(pdf,schema=schema)

# finally filter
##notice isnull function to column
df.filter((df['referee_id']!=2) | (df['referee_id'].isNull())).select('name').show()

# COMMAND ----------

# Using Sql
df.createOrReplaceTempView("Customers")
spark.sql("select name from Customers where referee_id!=2 or referee_id is null").show()
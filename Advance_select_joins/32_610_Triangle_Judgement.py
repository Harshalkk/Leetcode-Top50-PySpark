# Databricks notebook source
# MAGIC %md
# MAGIC # [610. Triangle Judgement](https://leetcode.com/problems/triangle-judgement/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Triangle
# MAGIC
# MAGIC <pre>+-------------+------+
# MAGIC | Column Name | Type |
# MAGIC +-------------+------+
# MAGIC | x           | int  |
# MAGIC | y           | int  |
# MAGIC | z           | int  |
# MAGIC +-------------+------+</pre>
# MAGIC In SQL, (x, y, z) is the primary key column for this table.
# MAGIC Each row of this table contains the lengths of three line segments.
# MAGIC  
# MAGIC
# MAGIC Report for every three line segments whether they can form a triangle.
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
# MAGIC Triangle table:
# MAGIC <pre>+----+----+----+
# MAGIC | x  | y  | z  |
# MAGIC +----+----+----+
# MAGIC | 13 | 15 | 30 |
# MAGIC | 10 | 20 | 15 |
# MAGIC +----+----+----+</pre>
# MAGIC Output: 
# MAGIC <pre>+----+----+----+----------+
# MAGIC | x  | y  | z  | triangle |
# MAGIC +----+----+----+----------+
# MAGIC | 13 | 15 | 30 | No       |
# MAGIC | 10 | 20 | 15 | Yes      |
# MAGIC +----+----+----+----------+</pre>

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[13, 15, 30], [10, 20, 15]]
triangle = pd.DataFrame(data, columns=['x', 'y', 'z']).astype({'x': 'Int64', 'y': 'Int64', 'z': 'Int64'})

# COMMAND ----------

# Converting to pyspask schema

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

triangle_df = spark.createDataFrame(triangle)
triangle_df.show()

# COMMAND ----------

# in spark Dataframe

## USING UDF:
    # 0. Import types, (udf doesn't req import) 
    # 1. First create normal function using Python
    # 2. Then :
    #     someFunName = udf(pythFunName,<dataType>())
    # 3. Use someFunName(,,) directly in spark like in withColumn

import pyspark.sql.functions as f
import pyspark.sql.types as t

def isTriangle(x,y,z):
    if x+y>z and x+z>y and y+z>x:
        return 'Y'
    return 'N'

isTri=udf(isTriangle,t.StringType())

triangle_df.withColumn("triangle",isTri('x','y','z')).show()

# COMMAND ----------

### Using UDF in SQL

# 1. spark.udf.register("someFunName",pythFunName,<returnDataType>())
# 2. Use someFunName(,,) in select query

triangle_df.createOrReplaceTempView("Triangle")

spark.udf.register("isTri",isTriangle,t.StringType())

spark.sql(
    '''
    select x,y,z,isTri(x,y,z) as triangle
    from Triangle
    '''
).show()

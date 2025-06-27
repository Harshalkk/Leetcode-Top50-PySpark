# Databricks notebook source
# MAGIC %md
# MAGIC # [196. Delete Duplicate Emails](https://leetcode.com/problems/delete-duplicate-emails/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Person
# MAGIC
# MAGIC <pre>+-------------+---------+
# MAGIC | Column Name | Type    |
# MAGIC +-------------+---------+
# MAGIC | id          | int     |
# MAGIC | email       | varchar |
# MAGIC +-------------+---------+</pre>
# MAGIC id is the primary key (column with unique values) for this table.
# MAGIC Each row of this table contains an email. The emails will not contain uppercase letters.
# MAGIC  
# MAGIC
# MAGIC Write a solution to delete all duplicate emails, keeping only one unique email with the smallest id.
# MAGIC
# MAGIC For SQL users, please note that you are supposed to write a DELETE statement and not a SELECT one.
# MAGIC
# MAGIC For Pandas users, please note that you are supposed to modify Person in place.
# MAGIC
# MAGIC After running your script, the answer shown is the Person table. The driver will first compile and run your piece of code and then show the Person table. The final order of the Person table does not matter.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Person table:
# MAGIC <pre>+----+------------------+
# MAGIC | id | email            |
# MAGIC +----+------------------+
# MAGIC | 1  | john@example.com |
# MAGIC | 2  | bob@example.com  |
# MAGIC | 3  | john@example.com |
# MAGIC +----+------------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+----+------------------+
# MAGIC | id | email            |
# MAGIC +----+------------------+
# MAGIC | 1  | john@example.com |
# MAGIC | 2  | bob@example.com  |
# MAGIC +----+------------------+</pre>
# MAGIC Explanation: john@example.com is repeated two times. We keep the row with the smallest Id = 1.

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[1, 'john@example.com'], [2, 'bob@example.com'], [3, 'john@example.com']]
person = pd.DataFrame(data, columns=['id', 'email']).astype({'id': 'int64', 'email': 'object'})

# COMMAND ----------

# to spark dataframe

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

person_df = spark.createDataFrame(person)
person_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as f

# solving in spark dataframe

# spark does not support direct delete being OLAP system.
# One solution is to re-write the data after applying filter. (not done in this example)
person_df.withColumn("rn",f.row_number().over(Window.partitionBy(f.col("email")).orderBy(f.col("id")))) \
    .filter(f.col("rn")==1) \
    .drop("rn").show()

# COMMAND ----------

# solving in spark SQL
person_df.printSchema()
person_df.createOrReplaceTempView("person")
spark.sql(
    '''
    with cte as (
            select `id`,email,row_number() over(partition by email order by `id`) as rn
            from person
    )
    select `id`,email
    from cte where rn=1
    order by `id`
    '''
).show()

# COMMAND ----------

`# mysql solution for OLTP:
'''
delete
from person 
where id in (
    select 
        id 
    from 
        (select id,row_number() over (partition by email order by id) as rn from person) p 
    where p.rn>1
 );
'''

# COMMAND ----------

spark.stop()
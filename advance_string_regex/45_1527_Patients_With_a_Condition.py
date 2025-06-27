# Databricks notebook source
# MAGIC %md
# MAGIC # [1527. Patients With a Condition](https://leetcode.com/problems/patients-with-a-condition/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Patients
# MAGIC
# MAGIC <pre>+--------------+---------+
# MAGIC | Column Name  | Type    |
# MAGIC +--------------+---------+
# MAGIC | patient_id   | int     |
# MAGIC | patient_name | varchar |
# MAGIC | conditions   | varchar |
# MAGIC +--------------+---------+</pre>
# MAGIC patient_id is the primary key (column with unique values) for this table.
# MAGIC 'conditions' contains 0 or more code separated by spaces. 
# MAGIC This table contains information of the patients in the hospital.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the patient_id, patient_name, and conditions of the patients who have Type I Diabetes. Type I Diabetes always starts with DIAB1 prefix.
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
# MAGIC Patients table:
# MAGIC <pre>+------------+--------------+--------------+
# MAGIC | patient_id | patient_name | conditions   |
# MAGIC +------------+--------------+--------------+
# MAGIC | 1          | Daniel       | YFEV COUGH   |
# MAGIC | 2          | Alice        |              |
# MAGIC | 3          | Bob          | DIAB100 MYOP |
# MAGIC | 4          | George       | ACNE DIAB100 |
# MAGIC | 5          | Alain        | DIAB201      |
# MAGIC +------------+--------------+--------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------------+--------------+--------------+
# MAGIC | patient_id | patient_name | conditions   |
# MAGIC +------------+--------------+--------------+
# MAGIC | 3          | Bob          | DIAB100 MYOP |
# MAGIC | 4          | George       | ACNE DIAB100 | 
# MAGIC +------------+--------------+--------------+</pre>
# MAGIC Explanation: Bob and George both have a condition that starts with DIAB1.

# COMMAND ----------

# pandas schema

import pandas as pd

data = [[1, 'Daniel', 'YFEV COUGH'], [2, 'Alice', ''], [3, 'Bob', 'DIAB100 MYOP'], [4, 'George', 'ACNE DIAB100'],
        [5, 'Alain', 'DIAB201']]
patients = pd.DataFrame(data, columns=['patient_id', 'patient_name', 'conditions']).astype(
    {'patient_id': 'int64', 'patient_name': 'object', 'conditions': 'object'})

# COMMAND ----------

# to spark dataframe

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

patients_df = spark.createDataFrame(patients)
patients_df.show(truncate=False)

# COMMAND ----------

# solving in spark dataframe
import pyspark.sql.functions as f
patients_df.filter(f.col("conditions").ilike("%DIAB1%")).show()

# COMMAND ----------

# solving in spark sql

patients_df.createOrReplaceTempView("patients")
spark.sql(
    '''
    select patient_id,patient_name,conditions
    from patients
    where conditions like '%DIAB1%'
    '''
).show()
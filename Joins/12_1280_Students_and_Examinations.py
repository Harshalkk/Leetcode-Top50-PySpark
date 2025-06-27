# Databricks notebook source
# MAGIC %md
# MAGIC # [1280. Students and Examinations](https://leetcode.com/problems/students-and-examinations/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Students
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | student_id    | int     |
# MAGIC | student_name  | varchar |
# MAGIC +---------------+---------+</pre>
# MAGIC student_id is the primary key (column with unique values) for this table.
# MAGIC Each row of this table contains the ID and the name of one student in the school.
# MAGIC  
# MAGIC
# MAGIC Table: Subjects
# MAGIC
# MAGIC <pre>+--------------+---------+
# MAGIC | Column Name  | Type    |
# MAGIC +--------------+---------+
# MAGIC | subject_name | varchar |
# MAGIC +--------------+---------+</pre>
# MAGIC subject_name is the primary key (column with unique values) for this table.
# MAGIC Each row of this table contains the name of one subject in the school.
# MAGIC  
# MAGIC
# MAGIC Table: Examinations
# MAGIC
# MAGIC <pre>+--------------+---------+
# MAGIC | Column Name  | Type    |
# MAGIC +--------------+---------+
# MAGIC | student_id   | int     |
# MAGIC | subject_name | varchar |
# MAGIC +--------------+---------+</pre>
# MAGIC There is no primary key (column with unique values) for this table. It may contain duplicates.
# MAGIC Each student from the Students table takes every course from the Subjects table.
# MAGIC Each row of this table indicates that a student with ID student_id attended the exam of subject_name.
# MAGIC  
# MAGIC
# MAGIC Write a solution to find the number of times each student attended each exam.
# MAGIC
# MAGIC Return the result table ordered by student_id and subject_name.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Students table:
# MAGIC <pre>+------------+--------------+
# MAGIC | student_id | student_name |
# MAGIC +------------+--------------+
# MAGIC | 1          | Alice        |
# MAGIC | 2          | Bob          |
# MAGIC | 13         | John         |
# MAGIC | 6          | Alex         |
# MAGIC +------------+--------------+</pre>
# MAGIC Subjects table:
# MAGIC <pre>+--------------+
# MAGIC | subject_name |
# MAGIC +--------------+
# MAGIC | Math         |
# MAGIC | Physics      |
# MAGIC | Programming  |
# MAGIC +--------------+</pre>
# MAGIC Examinations table:
# MAGIC <pre>+------------+--------------+
# MAGIC | student_id | subject_name |
# MAGIC +------------+--------------+
# MAGIC | 1          | Math         |
# MAGIC | 1          | Physics      |
# MAGIC | 1          | Programming  |
# MAGIC | 2          | Programming  |
# MAGIC | 1          | Physics      |
# MAGIC | 1          | Math         |
# MAGIC | 13         | Math         |
# MAGIC | 13         | Programming  |
# MAGIC | 13         | Physics      |
# MAGIC | 2          | Math         |
# MAGIC | 1          | Math         |
# MAGIC +------------+--------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+------------+--------------+--------------+----------------+
# MAGIC | student_id | student_name | subject_name | attended_exams |
# MAGIC +------------+--------------+--------------+----------------+
# MAGIC | 1          | Alice        | Math         | 3              |
# MAGIC | 1          | Alice        | Physics      | 2              |
# MAGIC | 1          | Alice        | Programming  | 1              |
# MAGIC | 2          | Bob          | Math         | 1              |
# MAGIC | 2          | Bob          | Physics      | 0              |
# MAGIC | 2          | Bob          | Programming  | 1              |
# MAGIC | 6          | Alex         | Math         | 0              |
# MAGIC | 6          | Alex         | Physics      | 0              |
# MAGIC | 6          | Alex         | Programming  | 0              |
# MAGIC | 13         | John         | Math         | 1              |
# MAGIC | 13         | John         | Physics      | 1              |
# MAGIC | 13         | John         | Programming  | 1              |
# MAGIC +------------+--------------+--------------+----------------+</pre>
# MAGIC Explanation: 
# MAGIC The result table should contain all students and all subjects.
# MAGIC Alice attended the Math exam 3 times, the Physics exam 2 times, and the Programming exam 1 time.
# MAGIC Bob attended the Math exam 1 time, the Programming exam 1 time, and did not attend the Physics exam.
# MAGIC Alex did not attend any exams.
# MAGIC John attended the Math exam 1 time, the Physics exam 1 time, and the Programming exam 1 time.

# COMMAND ----------

# Pandas Schema

import pandas as pd

data = [[1, 'Alice'], [2, 'Bob'], [13, 'John'], [6, 'Alex']]
students = pd.DataFrame(data, columns=['student_id', 'student_name']).astype(
    {'student_id': 'Int64', 'student_name': 'object'})
data = [['Math'], ['Physics'], ['Programming']]
subjects = pd.DataFrame(data, columns=['subject_name']).astype({'subject_name': 'object'})
data = [[1, 'Math'], [1, 'Physics'], [1, 'Programming'], [2, 'Programming'], [1, 'Physics'], [1, 'Math'], [13, 'Math'],
        [13, 'Programming'], [13, 'Physics'], [2, 'Math'], [1, 'Math']]
examinations = pd.DataFrame(data, columns=['student_id', 'subject_name']).astype(
    {'student_id': 'Int64', 'subject_name': 'object'})

# COMMAND ----------

# Pyspark conversion

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

students_df = spark.createDataFrame(students)
subjects_df = spark.createDataFrame(subjects)
examinations_df = spark.createDataFrame(examinations)

# COMMAND ----------

import pyspark.sql.functions as f
### students_df student_id | student_name |
### subjects_df subject_name
### examinations_df student_id | subject_name

### O/P student_id | student_name | subject_name | attended_exams |

## Notice crossjoin
## notice groupby on student_name
## Final cols outputs will be columns in groupBy + cols in agg
students_df.alias("s").join(subjects_df.alias("s1"),how="cross") \
            .join(examinations_df.alias("e"),(f.col("s.student_id") == f.col("e.student_id")) & (f.col("s1.subject_name") == f.col("e.subject_name")),how="left") \
            .groupBy(f.col("s.student_id"),f.col("s.student_name"),f.col("s1.subject_name")) \
                  .agg(f.count(f.col("e.student_id")).alias("attended_exam")).show()

# COMMAND ----------

# In spark SQL
students_df.createOrReplaceTempView("student")
subjects_df.createOrReplaceTempView("subjects")
examinations_df.createOrReplaceTempView("exam")

spark.sql('''
            select s.student_id,s.student_name,count(e.student_id) as attended_exam
            from student s cross join subjects s1
                Left join exam e on s.student_id=e.student_id and s1.subject_name=e.subject_name
            group by s.student_id,s.student_name,s1.subject_name
        ''').show()

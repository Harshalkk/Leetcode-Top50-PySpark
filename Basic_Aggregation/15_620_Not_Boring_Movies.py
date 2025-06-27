# Databricks notebook source
# MAGIC %md
# MAGIC # [620. Not Boring Movies](https://leetcode.com/problems/not-boring-movies/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Cinema
# MAGIC
# MAGIC <pre>+----------------+----------+
# MAGIC | Column Name    | Type     |
# MAGIC +----------------+----------+
# MAGIC | id             | int      |
# MAGIC | movie          | varchar  |
# MAGIC | description    | varchar  |
# MAGIC | rating         | float    |
# MAGIC +----------------+----------+</pre>
# MAGIC id is the primary key (column with unique values) for this table.
# MAGIC Each row contains information about the name of a movie, its genre, and its rating.
# MAGIC rating is a 2 decimal places float in the range [0, 10]
# MAGIC  
# MAGIC
# MAGIC Write a solution to report the movies with an odd-numbered ID and a description that is not "boring".
# MAGIC
# MAGIC Return the result table ordered by rating in descending order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Cinema table:
# MAGIC <pre>+----+------------+-------------+--------+
# MAGIC | id | movie      | description | rating |
# MAGIC +----+------------+-------------+--------+
# MAGIC | 1  | War        | great 3D    | 8.9    |
# MAGIC | 2  | Science    | fiction     | 8.5    |
# MAGIC | 3  | irish      | boring      | 6.2    |
# MAGIC | 4  | Ice song   | Fantacy     | 8.6    |
# MAGIC | 5  | House card | Interesting | 9.1    |
# MAGIC +----+------------+-------------+--------+</pre>
# MAGIC Output: 
# MAGIC <pre>+----+------------+-------------+--------+
# MAGIC | id | movie      | description | rating |
# MAGIC +----+------------+-------------+--------+
# MAGIC | 5  | House card | Interesting | 9.1    |
# MAGIC | 1  | War        | great 3D    | 8.9    |
# MAGIC +----+------------+-------------+--------+</pre>
# MAGIC Explanation: 
# MAGIC We have three movies with odd-numbered IDs: 1, 3, and 5. The movie with ID = 3 is boring so we do not include it in the answer.

# COMMAND ----------

# Pandas schema

import pandas as pd

data = [[1, 'War', 'great 3D', 8.9], [2, 'Science', 'fiction', 8.5], [3, 'irish', 'boring', 6.2],
        [4, 'Ice song', 'Fantacy', 8.6], [5, 'House card', 'Interesting', 9.1]]
cinema = pd.DataFrame(data, columns=['id', 'movie', 'description', 'rating']).astype(
    {'id': 'Int64', 'movie': 'object', 'description': 'object', 'rating': 'Float64'})

# COMMAND ----------

#to pyspark df

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

cinema_df = spark.createDataFrame(cinema)
cinema_df.show(truncate=False)

# COMMAND ----------

# spark df solution
import pyspark.sql.functions as F
#  id | movie      | description | rating |
# +----+------------+-------------+--------+
# | 1  | War        | great 3D    | 8.9    |
# | 2  | Science    | fiction     | 8.5    |
# | 3  | irish      | boring      | 6.2    |
# | 4  | Ice song   | Fantacy     | 8.6    |
# | 5  | House card | Interesting | 9.1    |
# +----+------------+-------------+--------+

cinema_df.filter( (F.col('id')%2==1) & (F.col("description")!="boring") ) \
    .orderBy(F.desc(F.col('rating')))  \
        .show()


# COMMAND ----------

# spark SQL solution

cinema_df.createOrReplaceTempView("cinema")
spark.sql('''select * from cinema
          where id%2==1 and description!="boring"
          Order by rating desc
          ''').show()


# COMMAND ----------


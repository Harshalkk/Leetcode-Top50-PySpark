# Databricks notebook source
# MAGIC %md
# MAGIC # [1341. Movie Rating](https://leetcode.com/problems/movie-rating/description/?envType=study-plan-v2&envId=top-sql-50)

# COMMAND ----------

# MAGIC %md
# MAGIC Table: Movies
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | movie_id      | int     |
# MAGIC | title         | varchar |
# MAGIC +---------------+---------+</pre>
# MAGIC movie_id is the primary key (column with unique values) for this table.
# MAGIC title is the name of the movie.
# MAGIC  
# MAGIC
# MAGIC Table: Users
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | user_id       | int     |
# MAGIC | name          | varchar |
# MAGIC +---------------+---------+</pre>
# MAGIC user_id is the primary key (column with unique values) for this table.
# MAGIC  
# MAGIC
# MAGIC Table: MovieRating
# MAGIC
# MAGIC <pre>+---------------+---------+
# MAGIC | Column Name   | Type    |
# MAGIC +---------------+---------+
# MAGIC | movie_id      | int     |
# MAGIC | user_id       | int     |
# MAGIC | rating        | int     |
# MAGIC | created_at    | date    |
# MAGIC +---------------+---------+</pre>
# MAGIC (movie_id, user_id) is the primary key (column with unique values) for this table.
# MAGIC This table contains the rating of a movie by a user in their review.
# MAGIC created_at is the user's review date. 
# MAGIC  
# MAGIC
# MAGIC Write a solution to:
# MAGIC
# MAGIC Find the name of the user who has rated the greatest number of movies. In case of a tie, return the lexicographically smaller user name.
# MAGIC Find the movie name with the highest average rating in February 2020. In case of a tie, return the lexicographically smaller movie name.
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Movies table:
# MAGIC <pre>+-------------+--------------+
# MAGIC | movie_id    |  title       |
# MAGIC +-------------+--------------+
# MAGIC | 1           | Avengers     |
# MAGIC | 2           | Frozen 2     |
# MAGIC | 3           | Joker        |
# MAGIC +-------------+--------------+</pre>
# MAGIC Users table:
# MAGIC <pre>+-------------+--------------+
# MAGIC | user_id     |  name        |
# MAGIC +-------------+--------------+
# MAGIC | 1           | Daniel       |
# MAGIC | 2           | Monica       |
# MAGIC | 3           | Maria        |
# MAGIC | 4           | James        |
# MAGIC +-------------+--------------+</pre>
# MAGIC MovieRating table:
# MAGIC <pre>+-------------+--------------+--------------+-------------+
# MAGIC | movie_id    | user_id      | rating       | created_at  |
# MAGIC +-------------+--------------+--------------+-------------+
# MAGIC | 1           | 1            | 3            | 2020-01-12  |
# MAGIC | 1           | 2            | 4            | 2020-02-11  |
# MAGIC | 1           | 3            | 2            | 2020-02-12  |
# MAGIC | 1           | 4            | 1            | 2020-01-01  |
# MAGIC | 2           | 1            | 5            | 2020-02-17  | 
# MAGIC | 2           | 2            | 2            | 2020-02-01  | 
# MAGIC | 2           | 3            | 2            | 2020-03-01  |
# MAGIC | 3           | 1            | 3            | 2020-02-22  | 
# MAGIC | 3           | 2            | 4            | 2020-02-25  | 
# MAGIC +-------------+--------------+--------------+-------------+</pre>
# MAGIC Output: 
# MAGIC <pre>+--------------+
# MAGIC | results      |
# MAGIC +--------------+
# MAGIC | Daniel       |
# MAGIC | Frozen 2     |
# MAGIC +--------------+</pre>
# MAGIC Explanation: 
# MAGIC Daniel and Monica have rated 3 movies ("Avengers", "Frozen 2" and "Joker") but Daniel is smaller lexicographically.
# MAGIC Frozen 2 and Joker have a rating average of 3.5 in February but Frozen 2 is smaller lexicographically.

# COMMAND ----------

# Pandas schema
import pandas as pd

data = [[1, 'Avengers'], [2, 'Frozen 2'], [3, 'Joker']]
movies = pd.DataFrame(data, columns=['movie_id', 'title']).astype({'movie_id': 'Int64', 'title': 'object'})
data = [[1, 'Daniel'], [2, 'Monica'], [3, 'Maria'], [4, 'James']]
users = pd.DataFrame(data, columns=['user_id', 'name']).astype({'user_id': 'Int64', 'name': 'object'})
data = [[1, 1, 3, '2020-01-12'], [1, 2, 4, '2020-02-11'], [1, 3, 2, '2020-02-12'], [1, 4, 1, '2020-01-01'],
        [2, 1, 5, '2020-02-17'], [2, 2, 2, '2020-02-01'], [2, 3, 2, '2020-03-01'], [3, 1, 3, '2020-02-22'],
        [3, 2, 4, '2020-02-25']]
movie_rating = pd.DataFrame(data, columns=['movie_id', 'user_id', 'rating', 'created_at']).astype(
    {'movie_id': 'Int64', 'user_id': 'Int64', 'rating': 'Int64', 'created_at': 'datetime64[ns]'})

# COMMAND ----------

#to spark dataframe
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

movies_df = spark.createDataFrame(movies)
users_df = spark.createDataFrame(users)
movie_rating_df = spark.createDataFrame(movie_rating)

# COMMAND ----------

movies_df.show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

movie_rating_df.show()

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as f
# movies
# | movie_id    |  title       |
# | 1           | Avengers     |
# Users table:
# +-------------+--------------+
# | user_id     |  name        |
# +-------------+--------------+
# | 1           | Daniel       |
# MovieRating table:
# +-------------+--------------+--------------+-------------+
# | movie_id    | user_id      | rating       | created_at  |
# +-------------+--------------+--------------+-------------+
# | 1           | 1            | 3            | 2020-01-12  |
# Output:
# +--------------+
# | results      |


## Get max ratings person

## Notice we have to collect ?
result_1=users_df.alias('u').join(movie_rating_df.alias('m'),on="user_id",how="inner") \
  .groupBy(f.col("u.user_id"),f.col("u.name")) \
  .agg(f.count("m.movie_id").alias("no_of_ratings")) \
    .orderBy(f.desc(f.col("no_of_ratings"))) \
        .select("u.name") \
          .limit(1).collect()[0][0] 
        
# print(result_1)
result_2=movie_rating_df.alias('m') \
  .join(movies_df.alias("m1"),on=["movie_id"]) \
    .filter(f.col("m.created_at").between("2020-02-01","2020-02-29")) \
  .groupBy(f.col("m.movie_id"),f.col("m1.title")) \
    .agg(f.avg(f.col("m.rating")).alias("total_ratings")) \
      .orderBy(f.desc("total_ratings")) \
        .limit(1) \
        .select("m1.title") \
        .collect()[0][0]

data=[[result_1],[result_2]]
spark.createDataFrame(data=data,schema=["result"]).show()

# COMMAND ----------

# In spark SQL

movies_df.createOrReplaceTempView('movies')
movie_rating_df.createOrReplaceTempView('MovieRating')
users_df.createOrReplaceTempView('users')

##notice we can't have union all with direct select, it needs to wrapperd around outer select ,
## as direct subqueries can't have limit and order by in UNION, you need to wrap them around another select without limit,order by 
spark.sql(
    '''
    select * from
    (
    select u.name as result
    from MovieRating m join users u on u.user_id = m.user_id
    group by u.user_id, u.name
    order by count(m.movie_id) desc
    limit 1
)
    UNION ALL
select * from 
(
    select m2.title as result
    from MovieRating m1 join movies m2 on m1.movie_id = m2.movie_id
    where m1.created_at between "2020-02-01" and "2020-02-29"
    group by m1.movie_id, m2.title
    order by avg(m1.rating) desc
    limit 1
)
    '''
).show()

# COMMAND ----------


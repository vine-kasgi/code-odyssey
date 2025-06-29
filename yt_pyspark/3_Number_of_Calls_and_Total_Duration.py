
"""
    Number of Calls and Total Duration
        You are given a table that logs phone call records between two individuals, including the caller (`from_id`), the recipient (`to_id`), 
        and the duration of the call in seconds. For the purpose of this task, you are required to group the records by unique person-to-person pairs, 
        regardless of direction (i.e., a call from 10 to 20 is the same as a call from 20 to 10). 
        Then, compute the total number of calls and the total duration for each unique pair.

    Table: calls

        Column Name | Type
        from_id     | INTEGER
        to_id       | INTEGER
        duration    | INTEGER

    Sample Input:

        from_id | to_id | duration
        10      | 20    | 58
        20      | 10    | 12
        10      | 30    | 20
        30      | 40    | 100
        30      | 40    | 200
        30      | 40    | 200
        40      | 30    | 500

    Expected Output:

        person1 | person2 | call_count | total_duration
        10      | 20      | 2          | 70
        10      | 30      | 1          | 20
        30      | 40      | 4          | 1000

    Explanation:

    - There are two calls between 10 and 20 (in either direction), totaling 70 seconds.
    - One call exists between 10 and 30, lasting 20 seconds.
    - Four calls between 30 and 40 (in either direction), totaling 1000 seconds.
"""
# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark: SparkSession = SparkSession.builder.appName("consecutive_event_summary").getOrCreate()

# Required Imports
from pyspark.sql.functions import col,row_number,date_format,to_date,lag,when,coalesce,sum,first,last,lead,least,greatest,count
from pyspark.sql.window import Window


data = [
    (10,20,58), 
    (20,10,12), 
    (10,30,20), 
    (30,40,100), 
    (30,40,200), 
    (30,40,200), 
    (40,30,500)
]

schema = ["from_id", "to_id", "duration"]

df : DataFrame = spark.createDataFrame(data=data,schema=schema)

# PySpark way of doing it
call_logs_df = df
# way 1 : old sckool
normalized_calls_df = call_logs_df.withColumn('person1', when(col('from_id')<col('to_id'),col('from_id')).otherwise(col('to_id'))) \
            .withColumn('person2',when(col('to_id')<col('from_id'),col('from_id')).otherwise(col('to_id')))
            
aggregated_call_df = normalized_calls_df.groupBy(['person1','person2']).agg(count('*').alias('call_count'),sum('duration').alias('total_duration'))
aggregated_call_df.show()

# way 2 : clean cut genz way

normalized_calls_df = call_logs_df.withColumn('person1',least(col('from_id'),col('to_id'))) \
            .withColumn('person2', greatest(col('from_id'),col('to_id')))

aggregated_call_df = normalized_calls_df.groupBy(col('person1'),col('person2')) \
            .agg(count('*').alias('call_count'),
                 sum('duration').alias('total_duration'))
aggregated_call_df.show()

# SparkSql way of doing it
df.createOrReplaceTempView('calls')

spark.sql("""
            with cte1 as(
                        select *
                        , CASE WHEN from_id < to_id then from_id else to_id end person1
                        , CASE WHEN to_id > from_id then to_id else from_id end person2
                        from calls)
            select person1, person2, count(*) as call_count, sum(duration) as total_duration from cte1 group by person1,person2
          """).show()

spark.stop()
# URL : https://www.youtube.com/watch?v=VsM7cqa2fBs&list=PLgPb8HXOGtsTv26lz_sXFyqkDiOXG-ucV&index=1

"""
    Consecutive Event Summary

    Given a list of tuples containing dates and corresponding event statuses ("Won" or "Lost"), 
    write a program to group consecutive entries with the same status into separate event blocks. 
    For each block, return the event status along with the start and end dates of that block.

    | Column Name  | Type    |
    | -------------| ------- |
    | event_date   | DATE    |
    | event_status | VARCHAR |


    | event_date  | event_status  |
    | ----------- | ------------- |
    | 2020-06-01  | Won           |
    | 2020-06-02  | Won           |
    | 2020-06-03  | Won           |
    | 2020-06-04  | Lost          |
    | 2020-06-05  | Lost          |
    | 2020-06-06  | Lost          |
    | 2020-06-07  | Won           |

    | event_status  | start_date  | end_date   |
    | ------------- | ----------- | ---------- |
    | Won           | 2020-06-01  | 2020-06-03 |
    | Lost          | 2020-06-04  | 2020-06-06 |
    | Won           | 2020-06-07  | 2020-06-07 |


"""

# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark: SparkSession = SparkSession.builder.appName("consecutive_event_summary").getOrCreate()

# Required Imports
from pyspark.sql.functions import col,row_number,date_format,to_date,lag,when,coalesce,sum,first,last
from pyspark.sql.window import Window

# Required DataFrame
data = [
    ("2020-06-01","Won"),
    ("2020-06-02","Won"),
    ("2020-06-03","Won"),
    ("2020-06-04","Lost"),
    ("2020-06-05","Lost"),
    ("2020-06-06","Lost"),
    ("2020-06-07","Won")
]
schema = ('event_date string, event_status string')

df: DataFrame = spark.createDataFrame(data=data,schema=schema)
df = df.withColumn('event_date',to_date(col('event_date'),'yyyy-MM-dd'))

# Define window for ordered comparison
date_order_window = Window.partitionBy().orderBy('event_date')

# Mark where a new streak starts (status change from previous row)
df_with_streak_flag = df.withColumn('is_new_streak', 
                       when(lag('event_status').over(date_order_window) != col('event_status'), 1).otherwise(0))

# Assign a group ID to each streak using a cumulative sum
df_with_streak_id = df_with_streak_flag.withColumn('event_grp',sum('is_new_streak').over(date_order_window))

# Aggregate each streak group to get its start and end date
streak_summary_df  = df_with_streak_id.groupBy('event_grp','event_status').agg(first('event_date').alias('start_date'),last('event_date').alias('end_date')).drop('event_grp')

# Show final result
streak_summary_df.show()


spark.stop()
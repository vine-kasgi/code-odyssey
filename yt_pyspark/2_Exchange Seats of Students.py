# URL : https://youtu.be/JBiqD3umB6s?si=KEKzBX5KH5iojEzb

"""
    Exchange Seats of Students
    You are given a table that contains a list of students sitting in a sequential order by their id. For the purpose of this task, you are required to swap the seats of every pair of adjacent students. If there is an odd number of students, the last student should remain in their original position.

    Table: students


    Column Name | Type
    id          | INTEGER
    student     | STRING


    id | student
    1 | Alice
    2 | Bob
    3 | Charlie
    4 | David
    5 | Eve

    Expected Output:

    id | student
    1 | Bob
    2 | Alice
    3 | David
    4 | Charlie
    5 | Eve

    Explanation:

    Alice (id=1) swaps with Bob (id=2)

    Charlie (id=3) swaps with David (id=4)

    Eve (id=5) has no one to swap with, so remains unchanged

"""


# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark: SparkSession = SparkSession.builder.appName("consecutive_event_summary").getOrCreate()

# Required Imports
from pyspark.sql.functions import col,row_number,date_format,to_date,lag,when,coalesce,sum,first,last,lead
from pyspark.sql.window import Window

data = [
        (1, "Alice"),
        (2, "Bob"), 
        (3, "Charlie"), 
        (4, "David"), 
        (5, "Eve")
]
schema = ("seat_id int, student string")

df : DataFrame = spark.createDataFrame(data=data,schema=schema)
# Pyspark way of doing it

# Defining window
seat_window = Window.partitionBy().orderBy(col('seat_id'))

# Using lead and lag to get next and previous students
students_with_neighbours_df = df.withColumn('next_student', lead(col('student')).over(seat_window)) \
                                .withColumn('prev_student', lag(col('student')).over(seat_window))

# Apply seat-swapping
swapped_seats_df = students_with_neighbours_df.withColumn('swapped_student_name',
                                                          when(col('seat_id') % 2 == 1, coalesce(col('next_student'),col('student'))) \
                                                          .when(col('seat_id') % 2 == 0, coalesce(col('prev_student'),col('student'))))

# final output with renamed columns
final_seating_df = swapped_seats_df.select('seat_id',col('swapped_student_name').alias('student'))
final_seating_df.show()


# SparkSQL way of doing it
df.createOrReplaceTempView('students')
sql_df = spark.sql("""
            select *
            , CASE WHEN seat_id % 2 = 0 THEN coalesce(lag(student) over (order by seat_id),student)
                    WHEN seat_id % 2 = 1 THEN coalesce(lead(student) over (order by seat_id),student)
              END as new_arrangement  
          
           from students
          """)

final_sql_df = sql_df.drop(col('student')).withColumnRenamed('new_arrangement','student')
final_sql_df.show()



spark.stop()
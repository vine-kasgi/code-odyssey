# URL : https://youtu.be/Tjv881etqgY?si=7inS4smvGDwTK0e7

"""
    Employees with Above-Average Salaries in Their Departments
        You are given a table that contains employee information, including their id, name, salary, and department. 
        Your task is to find the names of employees who earn strictly more than the average salary of their respective department.

    Table: employees

        Column Name | Type
        Id          | INTEGER
        Name        | STRING
        Salary      | INTEGER
        Department  | STRING

    Sample Input:

        Id | Name  | Salary | Department
        1  | Joe   | 70000  | IT
        2  | Henry | 80000  | HR
        3  | Sam   | 60000  | IT
        4  | Max   | 90000  | HR

    Expected Output:

        Name
        ------
        Joe
        Max

    Explanation:

    - In the IT department, the average salary is (70000 + 60000)/2 = 65000. Joe earns 70000, which is more than the average.
    - In the HR department, the average salary is (80000 + 90000)/2 = 85000. Max earns 90000, which is more than the average.
    - Henry and Sam earn less than or equal to their department's average, so they are not included.
"""


# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark: SparkSession = SparkSession.builder.appName("consecutive_event_summary").getOrCreate()

# Required Imports
from pyspark.sql.functions import col,row_number,date_format,to_date,lag,when,coalesce,sum,first,last,lead,avg
from pyspark.sql.window import Window

data = [
    (1, "Joe", 90000, "IT"),
    (2, "Henry", 60000, "HR"),
    (3, "Sam", 92000, "IT"),
    (4, "Max", 50000, "HR"),
    (5, "Nina", 75000, "Finance"),
    (6, "Tom", 70000, "Finance"),
    (7, "Laura", 45000, "HR"),
    (8, "Evan", 98000, "IT"),
    (9, "Kate", 66000, "Finance"),
    (10, "Mark", 90000, "IT")
]

schema = "Id int, Name string, Salary int, Department string"

# PySpark way of doing it

df : DataFrame = spark.createDataFrame(data=data,schema=schema)

# Define Window Partitioned by Department
dept_window = Window.partitionBy(col('Department'))

# Add average salary Column
employee_with_avg_df  = df.withColumn('salary', col('salary').cast('double')) \
    .withColumn('avg_salary',avg(col('Salary')).over(dept_window))

# Filter employees earning above department average
high_earners_df  = employee_with_avg_df.filter(col('salary') > col('avg_salary')).select('Name')

# Result
high_earners_df.show()

# Spark-SQL way of doing it

df.createOrReplaceTempView('employees')

spark.sql(
    """
        with cte as (
                    select name
                            ,salary
                            ,cast(avg(salary) over (partition by department) as double) as avg_salary
                    from employees)
        select * from cte where salary > avg_salary
    """
).show()


spark.stop()
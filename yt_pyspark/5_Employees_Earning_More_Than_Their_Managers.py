# URL : https://youtu.be/1fVquYHIHig?si=Fa9tQ9ny7UC6SRxa

"""
    Employees Earning More Than Their Managers
        You are given a table of employee information. Each employee has an Id, a Name, a Salary, and a ManagerId 
        indicating the Id of their manager. Your task is to find the names of employees whose salary is strictly 
        greater than the salary of their manager.

    Table: Employee

        Column Name | Type
        Id          | INTEGER
        Name        | STRING
        Salary      | INTEGER
        ManagerId   | INTEGER

    Sample Input:

        Id | Name  | Salary | ManagerId
        1  | Joe   | 70000  | 3
        2  | Henry | 80000  | 4
        3  | Sam   | 60000  | NULL
        4  | Max   | 90000  | NULL

    Expected Output:

        Name
        ------
        Joe

    Explanation:

    - Joe earns 70000, and his manager is Sam (Id 3), who earns 60000. Since 70000 > 60000, Joe is included.
    - Henry earns 80000, and his manager is Max (Id 4), who earns 90000. Since 80000 < 90000, Henry is excluded.
    - Sam and Max have no managers (ManagerId is NULL), so they are excluded.

"""

# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark: SparkSession = SparkSession.builder.appName("consecutive_event_summary").getOrCreate()

# Required Imports
from pyspark.sql.functions import col,row_number,date_format,to_date,lag,when,coalesce,sum,first,last,lead,avg,count_distinct,count
from pyspark.sql.window import Window

# Employee Data
employee_data = [
    (1, "Joe", 70000, 3),
    (2, "Henry", 80000, 4),
    (3, "Sam", 60000, None),
    (4, "Max", 90000, None),
    (5, "Nina", 95000, 3),
    (6, "Tom", 58000, 3),
    (7, "Laura", 99000, 4)
]

# Schema
employee_schema = "Id int, Name string, Salary int, ManagerId int"

# Employee df
emp_df : DataFrame = spark.createDataFrame(data = employee_data, schema = employee_schema)

# Employee with manager salary
emp_with_manager_salary_df = emp_df.alias('e').join(emp_df.alias('m'), on= (col('e.ManagerId') == col('m.Id')), how='left')

# Employee with more salary than manager
result_df = emp_with_manager_salary_df.filter(col('e.salary') > col('m.salary')).select(col('e.Name'))

emp_with_manager_salary_df.show()

result_df.show()
"""
Table: Employees

+-------------+----------+
| Column Name | Type     |
+-------------+----------+
| employee_id | int      |
| name        | varchar  |
| manager_id  | int      |
| salary      | int      |
+-------------+----------+
In SQL, employee_id is the primary key for this table.
This table contains information about the employees, their salary, and the ID of their manager. Some employees do not have a manager (manager_id is null). 
 

Find the IDs of the employees whose salary is strictly less than $30000 and whose manager left the company. When a manager leaves the company, their information is deleted from the Employees table, but the reports still have their manager_id set to the manager that left.

Return the result table ordered by employee_id.

The result format is in the following example.

 

Example 1:

Input:  
Employees table:
+-------------+-----------+------------+--------+
| employee_id | name      | manager_id | salary |
+-------------+-----------+------------+--------+
| 3           | Mila      | 9          | 60301  |
| 12          | Antonella | null       | 31000  |
| 13          | Emery     | null       | 67084  |
| 1           | Kalel     | 11         | 21241  |
| 9           | Mikaela   | null       | 50937  |
| 11          | Joziah    | 6          | 28485  |
+-------------+-----------+------------+--------+
Output: 
+-------------+
| employee_id |
+-------------+
| 11          |
+-------------+

Explanation: 
The employees with a salary less than $30000 are 1 (Kalel) and 11 (Joziah).
Kalel's manager is employee 11, who is still in the company (Joziah).
Joziah's manager is employee 6, who left the company because there is no row for employee 6 as it was deleted.
"""

# Create Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("1978_Employees_Whose_Manager_Left_the_Company").getOrCreate()

# add Imports
from pyspark.sql.functions import col

# Create Required Dataframe
data = [
    (3, 'Mila', 9, 60301),
    (12, 'Antonella', None, 31000),
    (13, 'Emery', None, 67084),
    (1, 'Kalel', 11, 21241),
    (9, 'Mikaela', None, 50937),
    (11, 'Joziah', 6, 28485),
]
schema = ('employee_id int,name string, manager_id int, salary int')
df = spark.createDataFrame(data,schema)

# Filter the salary and manager_id not nulls
emp_df = df.filter((df['salary'] < 30000) & (df['manager_id'].isNotNull()))

# 
# Self join for finding the ID of employee that are not in the organisation
join_df = emp_df.alias("e").join(df.alias('m'), col("e.manager_id") == col('m.employee_id'), "left" )

# Filtering the required emp_id

result_df = join_df.filter(col('m.employee_id').isNull()).select(col('e.employee_id')).orderBy("employee_id")
result_df.show()

spark.stop()
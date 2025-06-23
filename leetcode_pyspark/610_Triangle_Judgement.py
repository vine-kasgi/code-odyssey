"""
Table: Triangle

+-------------+------+
| Column Name | Type |
+-------------+------+
| x           | int  |
| y           | int  |
| z           | int  |
+-------------+------+
In SQL, (x, y, z) is the primary key column for this table.
Each row of this table contains the lengths of three line segments.
 

Report for every three line segments whether they can form a triangle.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Triangle table:
+----+----+----+
| x  | y  | z  |
+----+----+----+
| 13 | 15 | 30 |
| 10 | 20 | 15 |
+----+----+----+
Output: 
+----+----+----+----------+
| x  | y  | z  | triangle |
+----+----+----+----------+
| 13 | 15 | 30 | No       |
| 10 | 20 | 15 | Yes      |
+----+----+----+----------+

"""

# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark : SparkSession = SparkSession.builder.appName("610_Triangle_judgement").getOrCreate()

# Import methods
from pyspark.sql.functions import when,col

data = [
    (13,15,30),
    (10,20,15)
]
schema = ('x int,y int,z int')

df : DataFrame = spark.createDataFrame(data=data,schema=schema)

result = df.withColumn('Triangle',
                       when((col('x') == col('y'))  & (col('y') == col('z')),'Yes') \
                       .when(((col('x') + col('y') > col('z'))
                             & (col('x') + col('z') > col('y'))
                             & (col('z') + col('y') > col('x'))
                             ),'Yes') \
                             .otherwise('No')
                       )

result.show()

spark.stop()
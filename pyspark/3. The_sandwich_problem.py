# URL : https://youtu.be/PZMqTID8Dgg?si=jcSkK3OlszsnSGi9
"""
    Title: Sandwich Problem

    Question:

        Given a table NUM with the following schema:

        Column	|Type
        SN	    |INT
        NUMB	|INT

    This table contains a sequence of numbers with their serial number (SN) representing the order in which the numbers appear.

    Write a query to find all NUMB values that are "sandwiched" — i.e., a number that occurs more than once, and its first and last appearances have at least one different number in between (i.e., they are not adjacent).

    Return a result with a single column:

        NUMB
        -----
        4
        9

"""

from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lead

# Initialize Spark session
spark = SparkSession.builder.appName("CreateNUMDataset").getOrCreate()

# Define schema
schema = StructType([
    StructField("SN", IntegerType(), True),
    StructField("NUMB", IntegerType(), True)
])

# Step 3: Define data
data = [
    (1, 4),
    (2, 7),
    (3, 4),
    (4, 9),
    (5, 9),
    (6, 7),
    (7, 9),
    (8, 4)
]

# Create DataFrame
df : DataFrame = spark.createDataFrame(data, schema)

# Create a window specification
window_spec = Window.orderBy("NUMB")

# Use lead to get the third number after the current row

df_with_lead = df.withColumn('next_num', lead('Numb',2).over(window_spec))

# Filter the DataFrame to find "sandwiched" numbers
sandwich_df = df_with_lead.filter(col('next_num') == col('numb')).select('numb')

sandwich_df.show()
# ---
# title: Cumulative Revenue Till Current Day
# difficulty: Easy
# problem_type: pyspark
# source: 
# topic: window functions
# tags: pyspark, window, cumulative sum, revenue
# estimated_time: 10
# file_path: pyspark/8.Cumulative_revenue_till_current_day.py
# ---

"""
Problem Statement:

You're given transaction data for each customer. For each customer, calculate the cumulative revenue up to and including the current transaction, based on transaction date.

📘 Table: customer_transactions

| Column Name | Type   |
|-------------|--------|
| customer    | STRING |
| date        | DATE   |
| revenue     | INT    |

Each row represents a transaction made by a customer on a specific date.

🧾 Sample Input:

| customer | date       | revenue |
|----------|------------|---------|
| X        | 2024-01-01 | 100     |
| X        | 2024-01-03 | 200     |
| X        | 2024-01-05 | 300     |
| Y        | 2024-01-01 | 50      |
| Y        | 2024-01-02 | 100     |

✅ Expected Output:

| customer | date       | revenue | cumulative_total |
|----------|------------|---------|------------------|
| X        | 2024-01-01 | 100     | 100              |
| X        | 2024-01-03 | 200     | 300              |
| X        | 2024-01-05 | 300     | 600              |
| Y        | 2024-01-01 | 50      | 50               |
| Y        | 2024-01-02 | 100     | 150              |

Notes:
- Cumulative revenue is calculated per customer and ordered by transaction date.
"""

# Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Define schema
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
revenue_schema = StructType([
    StructField("customer", StringType(), True),
    StructField("date", DateType(), True),
    StructField("revenue", IntegerType(), True),
])

# Create data
from datetime import date
revenue_data = [
    ("X", date(2024, 1, 1), 100),
    ("X", date(2024, 1, 3), 200),
    ("X", date(2024, 1, 5), 300),
    ("Y", date(2024, 1, 1), 50),
    ("Y", date(2024, 1, 2), 100),
]

# Create Dataframe
df_revenue = spark.createDataFrame(revenue_data, schema=revenue_schema)
df_revenue.show()

# Solution
from pyspark.sql.window import Window
from pyspark.sql.functions import * # type: ignore

# create window
current_date_window = Window.partitionBy(col('customer')).orderBy(col('date')).rowsBetween(Window.unboundedPreceding,0)

# add cumulative total column over current_date_window

df_revenue.withColumn('cumulative_total', sum('revenue').over(current_date_window)).show()
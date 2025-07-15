# ---
# title: Moving Average Temperature Over 7 Days
# difficulty: Medium
# problem_type: pyspark
# source: 
# topic: window functions
# tags: pyspark, window, moving average, temperature
# estimated_time: 10
# file_path: pyspark/10.Moving_avg_temp_over_7_days.py
# ---

"""
Problem Statement:

Given daily temperature readings from different cities.
Compute the 7-day moving average temperature for each city.
7-days includes 3 days before, the current day, and 3 days after.

📘 Table: temperature_readings

| Column Name | Type      |
|-------------|-----------|
| city        | STRING    |
| reading_date| DATE      |
| temperature | INTEGER   |

Each row represents the recorded temperature of a city on a particular date.

🧾 Sample Input:

| city  | reading_date | temperature |
|-------|--------------|-------------|
| Delhi | 2024-01-01   | 15          |
| Delhi | 2024-01-02   | 16          |
| Delhi | 2024-01-03   | 14          |
| Delhi | 2024-01-04   | 17          |
| Delhi | 2024-01-05   | 18          |
| Delhi | 2024-01-06   | 16          |
| Delhi | 2024-01-07   | 15          |
| Delhi | 2024-01-08   | 19          |
| Delhi | 2024-01-09   | 20          |

✅ Expected Output:

+-----+------------+---------------+
| city|reading_date|moving_avg_temp|
+-----+------------+---------------+
|Delhi|  2024-01-01|           15.5|
|Delhi|  2024-01-02|           16.0|
|Delhi|  2024-01-03|           16.0|
|Delhi|  2024-01-04|          15.86|
|Delhi|  2024-01-05|          16.43|
|Delhi|  2024-01-06|           17.0|
|Delhi|  2024-01-07|           17.5|
|Delhi|  2024-01-08|           17.6|
|Delhi|  2024-01-09|           17.5|
+-----+------------+---------------+

Note:
- Values are rounded to 2 decimal places.
- The 7-day window is centered on each date, with up to 3 prior and 3 following days (if available).
"""

# Create spark session
from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.getOrCreate()

# Define schema
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
temp_schema = StructType([
    StructField("city", StringType(), True),
    StructField("date", DateType(), True),
    StructField("temperature", IntegerType(), True),
])

# Create data
from datetime import date
temp_data = [
    ("Delhi", date(2024, 1, 1), 15),
    ("Delhi", date(2024, 1, 2), 16),
    ("Delhi", date(2024, 1, 3), 14),
    ("Delhi", date(2024, 1, 4), 17),
    ("Delhi", date(2024, 1, 5), 18),
    ("Delhi", date(2024, 1, 6), 16),
    ("Delhi", date(2024, 1, 7), 15),
    ("Delhi", date(2024, 1, 8), 19),
    ("Delhi", date(2024, 1, 9), 20),
]

# Create Dataframe
df_temp = spark.createDataFrame(temp_data, schema=temp_schema)

# Solution
from pyspark.sql.window import Window
from pyspark.sql.functions import * # type: ignore

# define 7 days window
seven_days_window = Window.partitionBy('city').orderBy('date').rowsBetween(-3,3)

# add avg column
df_temp_avg = df_temp.withColumn('moving_avg_temp', round(avg('temperature').over(seven_days_window),2)) \
                        .select(col('city'),col('date').alias('reading_date'), col('moving_avg_temp'))
df_temp_avg.show()
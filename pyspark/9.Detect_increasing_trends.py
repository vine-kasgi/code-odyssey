# ---
# title: Detect Increasing Trends (Previous Value Comparison)
# difficulty: Easy
# problem_type: pyspark
# source: 
# topic: window functions
# tags: pyspark, lag, stock prices, comparison
# estimated_time: 10
# file_path: pyspark/9.Detect_Increasing_Trends.py
# ---

"""
Problem Statement:

Given stock prices per company per day, flag whether the price increased compared to the previous day's price.

📘 Table: stock_prices

| Column Name | Type   |
|-------------|--------|
| ticker      | STRING |
| date        | DATE   |
| price       | INT    |

Each row represents the stock price of a company (`ticker`) on a specific date.

🧾 Sample Input:

| ticker | date       | price |
|--------|------------|-------|
| TSLA   | 2024-01-01 | 100   |
| TSLA   | 2024-01-02 | 105   |
| TSLA   | 2024-01-03 | 103   |
| AAPL   | 2024-01-01 | 150   |
| AAPL   | 2024-01-02 | 150   |

✅ Expected Output:

| ticker | date       | price | price_up |
|--------|------------|-------|----------|
| TSLA   | 2024-01-01 | 100   | NULL     |
| TSLA   | 2024-01-02 | 105   | True     |
| TSLA   | 2024-01-03 | 103   | False    |
| AAPL   | 2024-01-01 | 150   | NULL     |
| AAPL   | 2024-01-02 | 150   | False    |

Notes:
- `price_up` is:
    - `True` if today's price is greater than yesterday's,
    - `False` if it's less than or equal,
    - `NULL` for the first date of each ticker (no previous comparison).
"""

# Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Define schema
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
price_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("date", DateType(), True),
    StructField("price", IntegerType(), True),
])

# Create data
from datetime import date
price_data = [
    ("TSLA", date(2024, 1, 1), 100),
    ("TSLA", date(2024, 1, 2), 105),
    ("TSLA", date(2024, 1, 3), 103),
    ("AAPL", date(2024, 1, 1), 150),
    ("AAPL", date(2024, 1, 2), 150),
]

# Create Dataframe
df_price = spark.createDataFrame(price_data, schema=price_schema)

# Solution
from pyspark.sql.window import Window
from pyspark.sql.functions import * # type: ignore

df_price.show()
df_with_lag = df_price.withColumn('previous_price',(lag('price').over(Window.partitionBy('ticker').orderBy(col('date').asc()))))

df_with_lag.show()

df_with_lag.withColumn('price_up',when(col('previous_price') < col('price'), True).otherwise(False)).drop('previous_price').show()

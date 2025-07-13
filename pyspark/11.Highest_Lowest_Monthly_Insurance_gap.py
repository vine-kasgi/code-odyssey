# ---
# title: Max-Min Monthly Issuance Gap by Credit Card
# difficulty: Medium
# problem_type: pyspark
# source: https://youtu.be/_kryVujs22w?si=lAZ5txALYy7GhTn0
# topic: aggregation
# tags: pyspark, groupby, max-min, window
# estimated_time: 15
# file_path: pyspark/11.Highest_Lowest_Monthly_Insurance_gap.py
# ---

"""
JPMorgan Chase tracks the monthly issuance of its various credit cards across different months and years. 
You're given a table credit_card_issuance containing the monthly number of cards issued for each credit card.

Write a SQL query to report the name of each credit card along with the difference between the highest and lowest monthly issued amounts.
Finally, order the results by the difference in descending order, so the card with the largest issuance fluctuation appears first.

📘 Table: credit_card_issuance

| Column Name   | Type        |
| --------------| ----------- |
| card_name     | VARCHAR(50) |
| issued_amount | INT         |
| issue_month   | TINYINT     |
| issue_year    | YEAR        |

Each row represents the number of cards issued for a particular card in a given month and year.
There will be no duplicate records for a given card in the same month and year.

✅ Expected Output:

+--------------------+------------+
|           card_name|issuance_gap|
+--------------------+------------+
|  Chase Freedom Flex|       15000|
|Chase Sapphire Re...|       10000|
+--------------------+------------+
"""
# Create spark session
from pyspark.sql import SparkSession
spark: SparkSession = SparkSession.builder.getOrCreate()

# Define schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("card_name", StringType(), False),
    StructField("issued_amount", IntegerType(), False),
    StructField("issue_month", IntegerType(), False),
    StructField("issue_year", IntegerType(), False)
])

# Create data
data = [
    ("Chase Freedom Flex", 55000, 1, 2021),
    ("Chase Freedom Flex", 60000, 2, 2021),
    ("Chase Freedom Flex", 65000, 3, 2021),
    ("Chase Freedom Flex", 70000, 4, 2021),
    ("Chase Sapphire Reserve", 170000, 1, 2021),
    ("Chase Sapphire Reserve", 175000, 2, 2021),
    ("Chase Sapphire Reserve", 180000, 3, 2021)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Solution
from pyspark.sql.functions import *  # type: ignore

df.show()
# Aggregation logic
df_with_min_max = df.groupBy('card_name').agg(
    min('issued_amount').alias('Min_issue'),
    max('issued_amount').alias('Max_issue')
)

result_df = df_with_min_max.selectExpr(
    'card_name', '(max_issue - min_issue) as issuance_gap'
).orderBy(col('issuance_gap').desc())

result_df.show()

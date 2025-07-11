"""
    Cumulative Revenue Till Current Day

    Description:
        You're given transaction data for each customer. 
        For each customer, calculate the cumulative revenue up to and including the current transaction, based on transaction date.

    Input:
        +----------+------------+----------+
        | customer |    date    | revenue  |
        +----------+------------+----------+
        |    X     | 2024-01-01 |   100    |
        |    X     | 2024-01-03 |   200    |
        |    X     | 2024-01-05 |   300    |
        |    Y     | 2024-01-01 |   50     |
        |    Y     | 2024-01-02 |   100    |
        +----------+------------+----------+

    Output:
        +----------+------------+----------+------------------+
        | customer |    date    | revenue  | cumulative_total |
        +----------+------------+----------+------------------+
        |    X     | 2024-01-01 |   100    | 100              |
        |    X     | 2024-01-03 |   200    | 300              |
        |    X     | 2024-01-05 |   300    | 600              |
        |    Y     | 2024-01-01 |   50     | 50               |
        |    Y     | 2024-01-02 |   100    | 150              |

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from datetime import date
from pyspark.sql.window import Window
from pyspark.sql.functions import * # type: ignore

spark = SparkSession.builder.getOrCreate()
revenue_data = [
    ("X", date(2024, 1, 1), 100),
    ("X", date(2024, 1, 3), 200),
    ("X", date(2024, 1, 5), 300),
    ("Y", date(2024, 1, 1), 50),
    ("Y", date(2024, 1, 2), 100),
]

revenue_schema = StructType([
    StructField("customer", StringType(), True),
    StructField("date", DateType(), True),
    StructField("revenue", IntegerType(), True),
])

df_revenue = spark.createDataFrame(revenue_data, schema=revenue_schema)
df_revenue.show()

# create window
current_date_window = Window.partitionBy(col('customer')).orderBy(col('date')).rowsBetween(Window.unboundedPreceding,0)

# add cumulative total column over current_date_window

df_revenue.withColumn('cumulative_total', sum('revenue').over(current_date_window)).show()
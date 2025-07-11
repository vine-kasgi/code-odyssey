"""
    Detect Increasing Trends (Previous Value Comparison)

    Description:
        Given stock prices per company per day, flag whether the price increased compared to the previous day.
    Input:
        +--------+------------+--------+
        | ticker |    date    | price  |
        +--------+------------+--------+
        |   TSLA | 2024-01-01 |  100   |
        |   TSLA | 2024-01-02 |  105   |
        |   TSLA | 2024-01-03 |  103   |
        |   AAPL | 2024-01-01 |  150   |
        |   AAPL | 2024-01-02 |  150   |
        +--------+------------+--------+

    Output:
        +--------+------------+--------+--------------+
        | ticker |    date    | price  | price_up     |
        +--------+------------+--------+--------------+
        |   TSLA | 2024-01-01 |  100   | null         |
        |   TSLA | 2024-01-02 |  105   | True         |
        |   TSLA | 2024-01-03 |  103   | False        |
        |   AAPL | 2024-01-01 |  150   | null         |
        |   AAPL | 2024-01-02 |  150   | False        |

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from datetime import date
from pyspark.sql.window import Window
from pyspark.sql.functions import * # type: ignore

spark = SparkSession.builder.getOrCreate()
price_data = [
    ("TSLA", date(2024, 1, 1), 100),
    ("TSLA", date(2024, 1, 2), 105),
    ("TSLA", date(2024, 1, 3), 103),
    ("AAPL", date(2024, 1, 1), 150),
    ("AAPL", date(2024, 1, 2), 150),
]

price_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("date", DateType(), True),
    StructField("price", IntegerType(), True),
])

df_price = spark.createDataFrame(price_data, schema=price_schema)
df_price.show()
df_with_lag = df_price.withColumn('previous_price',(lag('price').over(Window.partitionBy('ticker').orderBy(col('date').asc()))))

df_with_lag.show()

df_with_lag.withColumn('price_up',when(col('previous_price') < col('price'), True).otherwise(False)).drop('previous_price').show()

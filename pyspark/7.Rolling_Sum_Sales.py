"""
    Rolling Sum of Sales
        
    Description:
        Given a PySpark DataFrame containing daily sales per store, compute the 3-day rolling sum for each store. 
        The window should include the current day, the previous day, and the next day.

    Input:
        +--------+------------+-------+
        | store  |    date    | sales |
        +--------+------------+-------+
        |   A    | 2024-01-01 |   100 |
        |   A    | 2024-01-02 |   150 |
        |   A    | 2024-01-03 |   200 |
        |   A    | 2024-01-04 |   300 |
        |   B    | 2024-01-01 |   400 |
        |   B    | 2024-01-02 |   500 |
        +--------+------------+-------+

    Output:
        +--------+------------+-------+------------+
        | store  |    date    | sales | rolling_sum|
        +--------+------------+-------+------------+
        |   A    | 2024-01-01 |   100 |        250 |
        |   A    | 2024-01-02 |   150 |        450 |
        |   A    | 2024-01-03 |   200 |        650 |
        |   A    | 2024-01-04 |   300 |        500 |
        |   B    | 2024-01-01 |   400 |        900 |
        |   B    | 2024-01-02 |   500 |        900 |

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from datetime import date
from pyspark.sql.window import Window
from pyspark.sql.functions import * # type: ignore

spark = SparkSession.builder.getOrCreate()

sales_data = [
    ("A", date(2024, 1, 1), 100),
    ("A", date(2024, 1, 2), 150),
    ("A", date(2024, 1, 3), 200),
    ("A", date(2024, 1, 4), 300),
    ("B", date(2024, 1, 1), 400),
    ("B", date(2024, 1, 2), 500),
]

sales_schema = StructType([
    StructField("store", StringType(), True),
    StructField("date", DateType(), True),
    StructField("sales", IntegerType(), True),
])

df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_sales.show()

# Create Window
three_day_window = Window.partitionBy(col('store')).orderBy(col('date')).rowsBetween(-1,1)

# add new column

df_sales.withColumn('rolling_sum', sum('sales').over(three_day_window)).show()
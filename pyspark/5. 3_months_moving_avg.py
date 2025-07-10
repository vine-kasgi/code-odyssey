"""
    "Monthly 3-Month Moving Average of Customer Orders"

    💬 Problem Description:
        You are given an orders table containing information about customer orders. Each row represents an order placed by a customer.

    The table contains the following columns:

        cust_id (INTEGER): ID of the customer.
        order_id (INTEGER): Unique ID of the order.
        order_date (DATE): The date when the order was placed.
        order_status (STRING): 'Y' if the order was successful, 'N' otherwise.

    Your task is to compute the 3-month moving average of successful orders (i.e., order_status = 'Y') per month across all customers.
    The 3-month moving average for a given month is calculated as the average number of successful orders per month over the current month and the previous two months.

    Return the result with the following columns:

        year_month (STRING): Year and month in YYYY-MM format

        monthly_successful_orders (INTEGER): Count of successful orders in that month

        moving_avg_3_months (FLOAT): 3-month moving average rounded to two decimal places   

    data = [
        (1, 101, date(2023, 1, 10), 'Y'),
        (2, 102, date(2023, 1, 15), 'N'),
        (3, 103, date(2023, 2, 5), 'Y'),
        (1, 104, date(2023, 2, 20), 'Y'),
        (2, 105, date(2023, 3, 12), 'Y'),
        (3, 106, date(2023, 3, 22), 'Y'),
        (1, 107, date(2023, 4, 1), 'Y'),
        (2, 108, date(2023, 4, 5), 'Y'),
        (3, 109, date(2023, 5, 10), 'Y'),
    ]

    # Schema: cust_id, order_id, order_date, order_status

    Output:
        +-----------+------------------------+----------------------+
        |year_month |monthly_successful_orders|moving_avg_3_months  |
        +-----------+------------------------+----------------------+
        |2023-01    |1                       |1.00                  |
        |2023-02    |2                       |1.50                  |
        |2023-03    |2                       |1.67                  |
        |2023-04    |2                       |2.00                  |
        |2023-05    |1                       |1.67                  |
        +-----------+------------------------+----------------------+

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date
from pyspark.sql.functions import col, lag, date_format, to_date
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data: (cust_id, order_id, order_date, order_status)
data = [
    (1, 101, date(2023, 1, 10), 'Y'),
    (2, 102, date(2023, 1, 15), 'N'),
    (3, 103, date(2023, 2, 5),  'Y'),
    (1, 104, date(2023, 2, 20), 'Y'),
    (2, 105, date(2023, 3, 12), 'Y'),
    (3, 106, date(2023, 3, 22), 'Y'),
    (1, 107, date(2023, 4, 1),  'Y'),
    (2, 108, date(2023, 4, 5),  'Y'),
    (3, 109, date(2023, 5, 10), 'Y'),
]

# Define schema
schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("order_status", StringType(), True),
])

# Create DataFrame
orders_df = spark.createDataFrame(data, schema)

# Show the dataset
orders_df.show(truncate=False)

# Filter the successful orders
successful_orders_df = orders_df.filter(col('order_status') == 'Y')

# Add new column year-month
new_orders_df = successful_orders_df.withColumn('year_month', date_format(col('order_date'),'yyyy-MM'))
new_orders_df.show()
# Create window spec
# window_spec = Window


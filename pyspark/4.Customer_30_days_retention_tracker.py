"""
    Customer 30-Day Retention Tracker

    💬 Problem Description:
        You are given an orders table with information about customer purchases. Each row represents a customer's order.
        The table contains the following columns:
        cust_id (INTEGER): ID of the customer.
        order_id (INTEGER): Unique ID of the order.
        order_date (DATE): The date when the order was placed.
        order_status (STRING): 'Y' if the order was successful, 'N' otherwise.

        A customer is considered retained if they have two or more successful orders (order_status = 'Y') such that the difference between any two successive successful orders is 30 days or less.

        Write a query (or PySpark DataFrame transformation) to find all customers who are retained.

        Return a list of retained customer IDs sorted in ascending order.


        data = [
                    (1, 101, date(2023, 1, 1), 'Y'),
                    (1, 102, date(2023, 1, 25), 'Y'),   # within 30 days of previous
                    (1, 103, date(2023, 3, 1), 'Y'),    # >30 days from 1/25
                    (2, 201, date(2023, 1, 10), 'Y'),
                    (2, 202, date(2023, 2, 15), 'N'),
                    (2, 203, date(2023, 2, 25), 'Y'),   # >30 days from 1/10
                    (3, 301, date(2023, 2, 1), 'Y'),
                    (3, 302, date(2023, 2, 25), 'Y'),   # within 30 days
                    (4, 401, date(2023, 1, 1), 'N'),
                    (4, 402, date(2023, 2, 1), 'N'),    # no successful orders
                ]

        schema = StructType([
                                StructField("cust_id", IntegerType(), True),
                                StructField("order_id", IntegerType(), True),
                                StructField("order_date", DateType(), True),
                                StructField("order_status", StringType(), True),
                            ])

        Expected Output:
        +--------+
        |cust_id|
        +--------+
        |       1|
        |       3|
        +--------+
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date
from pyspark.sql.functions import col, lead, datediff, when
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

data = [
    (1, 101, date(2023, 1, 1), 'Y'),
    (1, 102, date(2023, 1, 25), 'Y'),   # within 30 days of previous
    (1, 103, date(2023, 3, 1), 'Y'),    # >30 days from 1/25
    (2, 201, date(2023, 1, 10), 'Y'),
    (2, 202, date(2023, 2, 15), 'N'),
    (2, 203, date(2023, 2, 25), 'Y'),   # >30 days from 1/10
    (3, 301, date(2023, 2, 1), 'Y'),
    (3, 302, date(2023, 2, 25), 'Y'),   # within 30 days
    (4, 401, date(2023, 1, 1), 'N'),
    (4, 402, date(2023, 2, 1), 'N'),    # no successful orders
]

schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("order_status", StringType(), True),
])

orders_df = spark.createDataFrame(data, schema)

# Create a Window Spec
window_spec = Window.partitionBy('cust_id').orderBy('order_date')

# filter the successful orders
filtered_orders_df = orders_df.filter(col('order_status') == 'Y')

# add row_number col
rn_orders_df = filtered_orders_df.withColumn('next_odr_date', lead('order_date').over(window_spec))

# retention -> diff between order_date and next_order_date
retention_check_df = rn_orders_df.withColumn('retention', when(datediff(col('next_odr_date'),col('order_date')) < 30,1).otherwise(0))

# result
result = retention_check_df.filter('retention ==  1').select('cust_id').distinct()

result.show()
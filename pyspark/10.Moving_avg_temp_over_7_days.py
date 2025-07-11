"""
    Moving Average Temperature Over 7 Days

    Description:
        Given daily temperature readings from different cities, 
        compute the 7-day moving average temperature for each city, 
        where the window includes 3 days before, the current day, and 3 days after.

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from datetime import date
from pyspark.sql.window import Window
from pyspark.sql.functions import * # type: ignore

spark = SparkSession.builder.getOrCreate()


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

temp_schema = StructType([
    StructField("city", StringType(), True),
    StructField("date", DateType(), True),
    StructField("temperature", IntegerType(), True),
])

df_temp = spark.createDataFrame(temp_data, schema=temp_schema)


# define 7 days window
seven_days_window = Window.partitionBy('city').orderBy('date').rowsBetween(-3,3)

# add avg column
df_temp_avg = df_temp.withColumn('seven_days_avg', round(avg('temperature').over(seven_days_window),2))
df_temp_avg.show()
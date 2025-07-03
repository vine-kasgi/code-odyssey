# URL : https://youtu.be/zO1QljAJ834?si=9y0rLyAUIL6k4ZU-

"""
    Customers Who Bought All Products
        You are given two tables: one that records product purchases by customers, 
        and another that lists all available products. 
        Your task is to find all customer IDs of those who have purchased **every** product listed in the Product table.

    Table: Customer

        Column Name  | Type
        customer_id  | INTEGER
        product_key  | INTEGER

    Table: Product

        Column Name  | Type
        product_key  | INTEGER

    Sample Input:

        Customer
        customer_id | product_key
        ------------+-------------
        1           | 5
        2           | 6
        3           | 5
        3           | 6
        1           | 6

        Product
        product_key
        -----------
        5
        6

    Expected Output:

        customer_id
        ------------
        1
        3

    Explanation:

    - There are 2 distinct products: 5 and 6.
    - Customer 1 has bought both product 5 and product 6.
    - Customer 3 has also bought both product 5 and product 6.
    - Customer 2 has only bought product 6 and is therefore excluded.
    - Only customers who have purchased *all* available products are included in the result.
"""

# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark: SparkSession = SparkSession.builder.appName("Customers_Who_Bought_All_Products").getOrCreate()

# Required Imports
from pyspark.sql.functions import col,row_number,date_format,to_date,lag,when,coalesce,sum,first,last,lead,avg,count_distinct,count
from pyspark.sql.window import Window

# Customer Data
customer_data = [
    (1, 101),
    (1, 102),
    (1, 103),
    (2, 101),
    (3, 102),
    (3, 101),
    (3, 103),
    (4, 101),
    (4, 102)
]
# Product Data
product_data = [
    (101,),
    (102,),
    (103,)
]

# Schema
customer_schema = "customer_id int, product_key int"
product_schema = "product_key int"

# Product df
product_df : DataFrame = spark.createDataFrame(data = product_data, schema= product_schema)

no_of_product = product_df.select('product_key').distinct().count()

print(no_of_product)

# Customer df

customer_df : DataFrame = spark.createDataFrame(data = customer_data,schema=customer_schema)

# Count distinct products purchased per customer
grouped_customer_count = customer_df.groupBy(col('customer_id')).agg(count_distinct('product_key').alias('count_of_products'))

# Filter customers who bought all products
filter_customer = grouped_customer_count.filter(col('count_of_products') == no_of_product).select(col('customer_id'))

# Show result
filter_customer.show()

spark.stop()
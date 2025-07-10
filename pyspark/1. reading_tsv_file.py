# Import OS
import os
pwd = os.getcwd()
file_path = pwd + '\\pyspark\\datasets\\population-by-age.tsv'
print(f"The file path is : {file_path}")

# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark: SparkSession = SparkSession.builder.appName("reading_tsv_file").getOrCreate()

# Required Imports
from pyspark.sql.functions import length,regexp_replace,split,col,row_number,date_format,to_date,lag,when,coalesce,sum,first,last,lead,avg,count
from pyspark.sql.window import Window

# Reading the population-by-age data 
raw_df : DataFrame = spark.read.csv(path= file_path,sep= r'\t' , header=True) 

# Filter only 2019 data 
raw_2019_df = raw_df.select(col(r'indic_de,geo\time'),col('2019 '))

# Get the required columns
final_population_2019_df = raw_2019_df.withColumn('Country_code', split(r'indic_de,geo\time', ',')[1]) \
    .withColumn('age_grp', regexp_replace((split(r'indic_de,geo\time', ',')[0]), 'PC_', '')) \
    .select(col('Country_code'),col('age_grp'),col('2019 ').alias('percentage_2019')) \
    .filter(length(col('Country_code')) == 2)

# Pivot the age_grp column
final_pivot_population_2019_df = final_population_2019_df.groupBy('country_code').pivot('age_grp').agg(sum('percentage_2019'))

# renaming the columns using spark sql
final_pivot_population_2019_df.createOrReplaceTempView('final_pivot_population_2019')

result = spark.sql("""
            select 
                country_code,
                Y0_14  AS age_group_0_14,
                Y15_24 AS age_group_15_24,
                Y25_49 AS age_group_25_49,
                Y50_64 AS age_group_50_64, 
                Y65_79 AS age_group_65_79,
                Y80_MAX AS age_group_80_max
            from 
                final_pivot_population_2019
            order by country_code
        """)

result.show()
spark.stop()
# Import OS
import os
pwd = os.getcwd()
file_path = pwd + '\pyspark\datasets\cricket_country_name.csv'
print(f"The file path is : {file_path}")

# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark: SparkSession = SparkSession.builder.appName("match_combination").getOrCreate()

# Required Imports
from pyspark.sql.functions import concat_ws,length,regexp_replace,split,col,row_number,date_format,to_date,lag,when,coalesce,sum,first,last,lead,avg,count_distinct,count
from pyspark.sql.window import Window

# Create Dataframe

country_df : DataFrame = spark.read.csv(path=file_path, sep= r',', header=True)

# Pyspark method
# Create a window
country_window = Window.partitionBy().orderBy(col('country'))

# Add row_number to country df
dictinct_country_df = country_df.distinct().withColumn('rn', row_number().over(country_window))

# Self join to generate unique combinations (team1 < team2)
joined_df = dictinct_country_df.alias('team_1').join(dictinct_country_df.alias('team_2'), on= (col('team_1.rn') < col('team_2.rn')))

# Create match column: "TEAM1 vs TEAM2"
result_df = joined_df.withColumn('teams', concat_ws(' vs ', col('team_1.country'), col('team_2.country')))


# Show sample matches
result_df.select('teams').show()

result_df.cache()
# Log total number of combinations
match_count = result_df.count()
print(f'Total unique matches generated: {match_count}')


# sql way of match combination
country_df.createOrReplaceTempView('country')
matches_df = spark.sql("""
                with cte as (
                                Select distinct(country)
                                        ,row_number() over (order by country) as rn
                                from 
                                    country
                            )
                select 
                    concat(t1.country, ' vs ' ,t2.country) as match
                from cte t1 
                join cte t2 
                    on t1.rn < t2.rn
          """)

matches_df.show()


spark.stop()
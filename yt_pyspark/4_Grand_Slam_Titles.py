"""
    Grand Slam Titles
        You are given two tables: one that lists tennis players with their IDs and names,
        and another that records the winners of the four Grand Slam championships each year.
        Your task is to count how many Grand Slam titles each player has won, and report
        only those players who have won at least one title.

    Table: Players

        Column Name   | Type
        player_id     | INTEGER
        player_name   | STRING

    Table: Championships

        Column Name   | Type
        year          | INTEGER
        Wimbledon     | INTEGER  -- player_id
        Fr_open       | INTEGER  -- player_id
        US_open       | INTEGER  -- player_id
        Au_open       | INTEGER  -- player_id

    Sample Input:

    Players:
        player_id | player_name
        ----------+-------------
        1         | Nadal
        2         | Federer
        3         | Novak

    Championships:
        year | Wimbledon | Fr_open | US_open | Au_open
        -----+-----------+---------+---------+---------
        2018 | 1         | 1       | 1       | 1
        2019 | 1         | 1       | 2       | 2
        2020 | 2         | 1       | 2       | 2

    Expected Output:

        player_id | player_name | grand_slams_count
        ----------+-------------+-------------------
        2         | Federer     | 5
        1         | Nadal       | 7

    Explanation:

    - Nadal (player_id = 1) won 7 titles:
      * Wimbledon: 2018, 2019 (2)
      * Fr_open: 2018, 2019, 2020 (3)
      * US_open: 2018 (1)
      * Au_open: 2018 (1)
    - Federer (player_id = 2) won 5 titles:
      * Wimbledon: 2020 (1)
      * US_open: 2019, 2020 (2)
      * Au_open: 2019, 2020 (2)
    - Novak (player_id = 3) didn't win any Grand Slam titles, so he is excluded.

"""

# Create Spark Session
from pyspark.sql import SparkSession,DataFrame
spark: SparkSession = SparkSession.builder.appName("consecutive_event_summary").getOrCreate()

# Required Imports
from pyspark.sql.functions import lit,col,row_number,date_format,to_date,lag,when,coalesce,sum,first,last,lead,avg,count_distinct,count
from pyspark.sql.window import Window

# Players Data
players_data = [
    (1, "Nadal"),
    (2, "Federer"),
    (3, "Novak"),
    (4, "Alcaraz")
]

# Championships Data
championships_data = [
    (2018, 1, 1, 1, 1),  # Nadal won all 4 in 2018
    (2019, 1, 1, 2, 2),  # Nadal 2, Federer 2
    (2020, 2, 1, 2, 2),  # Federer 3, Nadal 1
    (2021, 3, 3, 3, 3),  # Novak won all 4
    (2022, 4, 4, 4, 4)   # Alcaraz won all 4
]

# Schema
players_schema = "player_id int, player_name string"
championships_schema = "year int, Wimbledon int, Fr_open int, US_open int, Au_open int"

# Create players df
players_df : DataFrame = spark.createDataFrame(data = players_data, schema = players_schema)

# create championship df
championships_df:DataFrame = spark.createDataFrame(data = championships_data,schema = championships_schema)

# Method 1 : Using unpivot function
# unpivot the championship df
unpivot_df = championships_df.unpivot("year", ["Wimbledon","Fr_open","Us_open","Au_open"],"championship","player_id")

# Join the players df
joined_df = players_df.join(unpivot_df, ('player_id'), how= 'left').orderBy(['player_id','year'],ascending = [True,True])

# Result df
result_df = joined_df.groupBy('player_id','player_name').agg(count('championship').alias('grand_slams_count'))
result_df.show()

# Method 2 : Using Union ALL (Least Preferred)

unpivot_df_v2 = championships_df.select('year',lit("Wimbledon").alias('Championship'), col('Wimbledon').alias('player_id')) \
                                .unionAll(championships_df.select('year',lit("Fr_open").alias('Championship'), col('Fr_open').alias('player_id'))) \
                                .unionAll(championships_df.select('year',lit("Us_open").alias('Championship'), col('Us_open').alias('player_id'))) \
                                .unionAll(championships_df.select('year',lit("Au_open").alias('Championship'), col('Au_open').alias('player_id')))
# Join the players df
joined_df_v2 = players_df.join(unpivot_df_v2, ('player_id'), how= 'left').orderBy(['player_id','year'],ascending = [True,True])

# Result df
result_df_v2 = joined_df_v2.groupBy('player_id','player_name').agg(count('championship').alias('grand_slams_count'))
result_df_v2.show()

spark.stop()
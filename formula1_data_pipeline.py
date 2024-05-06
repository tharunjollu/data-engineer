import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, lit, coalesce, input_file_name, regexp_replace, to_date
from datetime import datetime
from pyspark.sql.functions import input_file_name

spark = SparkSession.builder.master("local[1]").appName("F1DataPipeline").getOrCreate()

# Read CSV files from the data folder
circuits_path = "../dataeng-exercise-de-2/data/circuits.csv"
circuits_df = spark.read.format("csv").option("header", "true").load(circuits_path)

status_path = "../dataeng-exercise-de-2/data/status.csv"
status_df = spark.read.format("csv").option("header", "true").load(status_path)

seasons_path = "../dataeng-exercise-de-2/data/seasons.csv"
seasons_df = spark.read.format("csv").option("header", "true").load(seasons_path)

results_path = "../dataeng-exercise-de-2/data/results.csv"
results_df = spark.read.format("csv").option("header", "true").load(results_path)

races_path = "../dataeng-exercise-de-2/data/races.csv"
races_df = spark.read.format("csv").option("header", "true").load(races_path)

qualifying_path = "../dataeng-exercise-de-2/data/qualifying.csv"
qualifying_df = spark.read.format("csv").option("header", "true").load(qualifying_path)

pit_stops_path = "../dataeng-exercise-de-2/data/pit_stops.csv"
pit_stops_df = spark.read.format("csv").option("header", "true").load(pit_stops_path)

lap_times_path = "../dataeng-exercise-de-2/data/lap_times.csv"
lap_times_df = spark.read.format("csv").option("header", "true").load(lap_times_path)

drivers_path = "../dataeng-exercise-de-2/data/drivers.csv"
drivers_df = spark.read.format("csv").option("header", "true").load(drivers_path)

# Basic exploration data analysis
# Display the schema and first few rows of each DataFrame
for df_name, df in [("circuits_df", circuits_df), ("status_df", status_df), ("seasons_df", seasons_df), ("results_df", results_df), ("races_df", races_df), ("qualifying_df", qualifying_df), ("pit_stops_df", pit_stops_df), ("lap_times_df", lap_times_df), ("drivers_df", drivers_df)]:
    print(f"Schema of {df_name}:")
    df.printSchema()
    print(f"First few rows of {df_name}:")
    df.show(5)

# Implement transformation steps
# 1. What was the average time each driver spent at the pit stop for any given race?
pit_stops_df = pit_stops_df.withColumn("race_year", col("raceId").cast("integer").cast("String").substr(0, 4))
pit_stops_df = pit_stops_df.withColumn("driver_code", coalesce(col("driverId"), lit("0")))
pit_stops_df = pit_stops_df.withColumn("time_in_seconds", col("milliseconds") / 1000)
pit_stops_df = pit_stops_df.groupBy("driver_code", "race_year").agg(avg("time_in_seconds").alias("avg_pit_stop_time"))

# 2. Insert the missing code (e.g: ALO for Alonso) for all drivers
drivers_df = drivers_df.withColumn("driver_code", regexp_replace(col("driverRef"), "[^A-Z]", ""))

# 3. Select a season from the data and determine who was the youngest and oldest at the start of the season and the end of the season
selected_season = 2021
season_df = seasons_df.filter(col("year") == selected_season)
season_drivers_df = results_df.join(drivers_df, on=["driverId"], how="inner").filter(col("raceId").isin(season_df.select("raceId").distinct().collect()[0][0]))
youngest_driver = season_drivers_df.orderBy(col("dob").asc()).first()
oldest_driver = season_drivers_df.orderBy(col("dob").desc()).first()

# 4. Which driver has the most wins and which driver has the most losses for each Grand Prix?
gp_wins_df = results_df.groupBy("raceId").agg(sum(when(col("position") == 1, 1).otherwise(0)).alias("wins"))
gp_losses_df = results_df.groupBy("raceId").agg(sum(when(col("position") != 1, 1).otherwise(0)).alias("losses"))
gp_wins_df = gp_wins_df.join(gp_losses_df, on=["raceId"], how="inner")

# Write the results to CSV files
for df, input_path in [(pit_stops_df, pit_stops_path), (gp_wins_df, results_path)]:
    output_folder = "output"
    output_path = f"{output_folder}/{input_path.split('/')[-1]}"
    df.write.format("csv").option("header", "true").save(output_path)

spark.stop()
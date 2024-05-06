import os
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat
from pyspark.sql.types import StringType

# Initialize SparkSession
spark = SparkSession.builder.master("local[1]").appName("formula1_pipeline_tests").getOrCreate()

# Define helper functions
def create_test_data():
    with open("data/status.csv", "w") as status_file, open("data/pit_stops.csv", "w") as pit_stops_file, open("data/results.csv", "w") as results_file, open("data/drivers.csv", "w") as drivers_file:
        status_file.write("driverId,startPosition,status\n1,1,1\n2,2,2")
        pit_stops_file.write("raceId,driverId,duration,pitStopPosition\n1,1,10,1\n1,2,15,2")
        results_file.write("raceId,driverId,position,time\n1,1,1,1000\n1,2,2,1015")
        drivers_file.write("driverId,forename,surname,code,dob\n1,Fernando,Alonso,ALO,1981-07-29\n2,Lewis,Hamilton,HAM,1985-01-07")

def compare_csv_files(expected_file, actual_file):
    with open(expected_file, "r") as expected_file_handle, open(actual_file, "r") as actual_file_handle:
        expected_lines = expected_file_handle.readlines()
        actual_lines = actual_file_handle.readlines()

        assert len(expected_lines) == len(actual_lines)

        for expected_line, actual_line in zip(expected_lines, actual_lines):
            assert expected_line.strip() == actual_line.strip()

def test_formula1_pipeline():
    # Create test data
    create_test_data()

    # Run the pipeline
    with tempfile.TemporaryDirectory() as temp_dir:
        os.chdir(temp_dir)
        os.system("python ../formula1_pipeline.py")

        # Compare the output files with the expected files
        for file_name in ["avg_pit_stop_time.csv", "drivers_with_code.csv", "youngest_driver.csv", "oldest_driver.csv", "driver_wins.csv", "driver_losses.csv", "drivers_with_name.csv", "most_common_start_pos.csv", "most_common_pit_stop_pos.csv", "top_10_youngest_drivers.csv", "top_10_oldest_drivers.csv", "most_drivers_in_race.csv", "top_10_fastest_races.csv", "top_10_slowest_races.csv"]:
            expected_file = f"output/{file_name}"
            actual_file = f"output/formula1_pipeline/{file_name}"
            compare_csv_files(expected_file, actual_file)

# Runthe tests
test_formula1_pipeline()
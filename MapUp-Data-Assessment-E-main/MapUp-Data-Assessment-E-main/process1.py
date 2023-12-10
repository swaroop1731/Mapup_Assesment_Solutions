import pyarrow.parquet as pq              # As the raw_date is in .parquet format we need to user pyarrow module to read the data
import pandas as pd                       # Pandas module to convert pyquert to pandas dataframe
from datetime import datetime, timedelta  # To perform operations on timestamp we need this module
 
######## READ THE DATA ########
raw_data = r'C:\Users\HP\Desktop\MapUp-Data-Assessment-E-main\MapUp-Data-Assessment-E-main\evaluation_data\input\raw_data.parquet'
output_dir = r'C:\Users\HP\Desktop\MapUp-Data-Assessment-E-main\MapUp-Data-Assessment-E-main\evaluation_data\output\process1'

###############################

# Read the Parquet file into a Pandas DataFrame
df = pd.read_parquet(raw_data)

# Convert timestamp to datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Calculate the difference between the trips
df['time_diff'] = df['timestamp'].diff()

# Check if the duration of trip is greater than 7 hours
df['new_trip'] = df['time_diff'] > timedelta(hours=7)

# Add the trip count
df['trip_id'] = df['new_trip'].cumsum()

# Drop the temporary columns if needed
df = df.drop(columns=['time_diff', 'new_trip'])

# Grouping the data by "unit" and "trip_id"
grouped = df.groupby(['unit', 'trip_id'])

# Process each group and save as CSV
for (unit, trip_id), group in grouped:
    # Create a new CSV file for each trip
    trip_file_path = f"{output_folder}/{unit}_{trip_id}.csv"
     # Save the group to the CSV file keeping index = False cause we dont want index to be added
    group.to_csv(trip_file_path, index=False)
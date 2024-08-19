import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import matplotlib.pyplot as plt
import plotly.graph_objects as go

def get_data_between_timestamps(start_timestamp, end_timestamp):
    # Connect to your PostgreSQL database
    conn = psycopg2.connect("dbname=mydatabase user=postgres password=postgres host=db")
    cursor = conn.cursor()

    # SQL query to select data between two timestamps
    query = """
    SELECT location_id, parameter_id, unit_id, custom_parameter, timestamp, value
    FROM locations
    WHERE timestamp BETWEEN %s AND %s
    ORDER BY timestamp ASC;
    """
    cursor.execute(query, (start_timestamp, end_timestamp))
    
    # Fetch all the records
    records = cursor.fetchall()

    # Define column names for the DataFrame
    column_names = ['location_id', 'parameter_id', 'unit_id', 'custom_parameter', 'timestamp', 'value']

    # Create the DataFrame
    df = pd.DataFrame(records, columns=column_names)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Create the mapping dictionary for units
    units_mapping = {
    "0": " ",
    "1": "C",
    "2": "F",
    "17": "psi",
    "19": "kPa",
    "20": "bar",
    "21": "mbar",
    "22": "mm Hg",
    "23": "in Hg",
    "24": "cm H₂O",
    "25": "in H₂O",
    "26": "torr",
    "27": "atm",
    "33": "mm",
    "34": "cm",
    "35": "m",
    "36": "km",
    "37": "in",
    "38": "ft",
    "49": "°",
    "65": "µS/cm",
    "66": "mS/cm",
    "81": "Ω-cm",
    "97": "psu",
    "241": "%",
    }

    # Create the mapping dictionary for parameters
    parameter_mapping = {
    1: "Temperature",
    2: "Pressure",
    3: "Depth",
    4: "Level: Depth to Water",
    5: "Level: Elevation",
    6: "Latitude",
    7: "Longitude",
    8: "Elevation",
    9: "Actual Conductivity",
    10: "Specific Conductivity",
    11: "Resistivity",
    12: "Salinity",
    13: "Total Dissolved Solids",
    14: "Density",
    16: "Baro",
    33: "Battery Level",
    }

    # Map the parameter_id to the parameter name
    df['parameter_id'] = df['parameter_id'].astype(int)
    df['parameter'] = df['parameter_id'].map(parameter_mapping)

    # Map the unit_id to the units name
    df['unit_id'] = df['unit_id'].astype(str)
    df['unit'] = df['unit_id'].map(units_mapping)

    # Convert the Unix timestamp to local readable datetime
    df['date'] = pd.to_datetime(df['timestamp'], unit='s')
    df['date_utc'] = pd.to_datetime(df['date'], utc=True)
    df['date'] = df['date_utc'].dt.tz_convert('America/Los_Angeles')

    df = df.sort_values(by='timestamp')

    return df

def get_usgs_data_between_dates(start_datetime, end_datetime):
    # Connect to your PostgreSQL database
    # Connect to your PostgreSQL database
    conn = psycopg2.connect("dbname=mydatabase user=postgres password=postgres host=db")
    cursor = conn.cursor()

    # SQL query to fetch data between the specified datetime values
    query = """
    SELECT site_code, site_name, latitude, longitude, variable_code, variable_name, unit_code, datetime, value, qualifier
    FROM usgs_readings
    WHERE datetime BETWEEN %s AND %s
    ORDER BY datetime ASC;
    """
    
    # Execute the query and fetch the data
    cursor.execute(query, (start_datetime, end_datetime))
    records = cursor.fetchall()

    # Define the column names for the DataFrame
    column_names = ['site_code', 'site_name', 'latitude', 'longitude', 'variable_code', 'variable_name', 'unit_code', 'datetime', 'value', 'qualifier']

    # Create the DataFrame
    df = pd.DataFrame(records, columns=column_names)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df

def get_noaa_tide_predictions(start_time, end_time):
    # Connect to your PostgreSQL database
    conn = psycopg2.connect("dbname=mydatabase user=postgres password=postgres host=db")
    cursor = conn.cursor()

    # SQL query to fetch data between start_time and end_time
    query = """
    SELECT prediction_time, value, type
    FROM noaa_tide_predictions
    WHERE prediction_time BETWEEN %s AND %s
    ORDER BY prediction_time ASC;
    """

    # Execute the query with the provided start_time and end_time
    cursor.execute(query, (start_time, end_time))

    # Fetch all rows from the query result
    rows = cursor.fetchall()

    # Define column names for the DataFrame
    column_names = ['prediction_time', 'value', 'type']

    # Create a pandas DataFrame from the fetched data
    df = pd.DataFrame(rows, columns=column_names)
    # Add a new column 'prediction_time_est' which is 'prediction_time' minus 7 hours
    # this entire thig need to handle time zone and daylight savings time
    df['prediction_time_est'] = df['prediction_time'] - timedelta(hours=7)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return df
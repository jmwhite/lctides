# The script that fetches data from USGS and NOAA APIs, processes it with pandas,
# and stores it in the PostgreSQL database.
import requests
import pandas as pd
import psycopg2
from datetime import datetime
import json

# Connect to the PostgreSQL database
conn = psycopg2.connect("dbname=mydatabase user=postgres password=postgres host=db")
cursor = conn.cursor()

# Create a table to store the data (if it doesn't already exist)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS usgs_readings (
        site_code VARCHAR(20),
        site_name VARCHAR(255),
        latitude FLOAT,
        longitude FLOAT,
        variable_code VARCHAR(10),
        variable_name VARCHAR(255),
        unit_code VARCHAR(10),
        datetime TIMESTAMP,
        value FLOAT,
        qualifier VARCHAR(10),
        PRIMARY KEY (site_code, variable_code, datetime)
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS noaa_tide_predictions (
        id SERIAL PRIMARY KEY,
        prediction_time TIMESTAMP NOT NULL,
        value FLOAT NOT NULL,
        type CHAR(1) NOT NULL
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS locations (
        location_id BIGINT,
        parameter_id VARCHAR(10),
        unit_id VARCHAR(10),
        custom_parameter BOOLEAN,
        timestamp BIGINT,
        value FLOAT,
        PRIMARY KEY (location_id, parameter_id, timestamp)
    )
''')

conn.commit()


def get_hydro_view_access_token(client_id, client_secret, hv_token_url):
    # Define the headers and data for the request
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    #print(token_url)
    #print(headers)
    #print(data)

    # Make the request to the token endpoint
    response = requests.post(hv_token_url, headers=headers, data=data)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        token_response = response.json()
        return token_response.get('access_token')
    else:
        # Handle error response
        print(f"Failed to obtain access token: {response.status_code} {response.text}")
        return None

def fetch_hydro_view_data(vh_data_url, start_time, location_id, access_token):
    import requests
    import time

    # Query to get the latest timestamp
    cursor.execute("SELECT MAX(timestamp) FROM locations;")
    result = cursor.fetchone()

    # Check if the result is None or contains a NULL value, and use the default timestamp if so
    latest_timestamp = result[0]+1 if result[0] is not None else start_time # type: ignore

    print(f"The latest timestamp in the database is: {latest_timestamp}")

    # Define the URL
    url = vh_data_url.format(location_id=location_id)
    authorization = "Bearer {access_token}".format(access_token=access_token)

    # Define the query parameters
    params = {
        "startTime": latest_timestamp
    }
    # Define the headers
    headers = {
        "accept": "application/json",
        "authorization": authorization
    }
    # Initialize an empty list to hold the data
    all_data = []

    while url:
        # Make the GET request
        response = requests.get(url, headers=headers, params=params)
        # Check if the request was successful
        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.status_code}")
            break

        # Add the data to the list (assuming JSON response)
        data = response.json()
        #print(data)
        all_data.append(data)

        # Check for the x-isi-next-page header to continue fetching
        next_page_token = response.headers.get("x-isi-next-page")
        time.sleep(5)
        print(next_page_token)
        if next_page_token:
            # Update the URL for the next request
            print("paging...")
            headers['X-ISI-Start-Page'] = next_page_token
        else:
            # Exit the loop if there is no next page
            break

    # Print the response status code and body
    #print(url)
    #print(authorization)
    #print(params)
    #print(response.status_code)
    #print("length = ", len(all_data))  # Assuming the response is in JSON format

    return all_data

def fetch_usgs_data(usgs_data_url, start_time):
    import requests
    import time

    # Query to get the latest timestamp
    cursor.execute("SELECT MAX(datetime) FROM usgs_readings;")
    result = cursor.fetchone()
    # Check if the result is None or contains a NULL value, and use the default timestamp if so
    latest_timestamp = result[0]  if result[0] is not None else datetime.utcfromtimestamp(int(start_time)) # type: ignore

    latest_timestamp = latest_timestamp + timedelta(seconds=1)
    start_time_str = latest_timestamp.strftime('%Y-%m-%dT%H:%MZ') 

    end_time = datetime.utcnow()
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%MZ') 

    # Define the base URL and the parameters
    params = {
        "format": "json",
        "sites": "12181000,12184700,12194000,12200500",
        "parameterCd": "00060,00065",
        "siteType": "ST",
        "siteStatus": "active",
        "startDT": start_time_str,
        "endDT": end_time_str
    }

    # Make the GET request
    response = requests.get(usgs_data_url, params=params)

    # Check the response status
    if response.status_code == 200:
        data = response.json()  # Parse the JSON response
    else:
        print(f"Error: {response.status_code}")

    print(f"The latest timestamp in the database is: {latest_timestamp}")
    return data

def process_and_store_usgs_data(data):
    #timestamp = datetime.now()
    for time_series in data['value']['timeSeries']:
        site_info = time_series['sourceInfo']
        variable_info = time_series['variable']
        values = time_series['values'][0]['value']

        site_code = site_info['siteCode'][0]['value']
        site_name = site_info['siteName']
        latitude = site_info['geoLocation']['geogLocation']['latitude']
        longitude = site_info['geoLocation']['geogLocation']['longitude']
        variable_code = variable_info['variableCode'][0]['value']
        variable_name = variable_info['variableName']
        unit_code = variable_info['unit']['unitCode']

        for reading in values:
            datetime = reading['dateTime']
            value = float(reading['value'])
            qualifier = reading['qualifiers'][0]

            insert_query = """
            INSERT INTO usgs_readings (
                site_code, site_name, latitude, longitude, variable_code, variable_name, unit_code, datetime, value, qualifier
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (site_code, variable_code, datetime) DO NOTHING;
            """
            cursor.execute(insert_query, (
                site_code, site_name, latitude, longitude, variable_code, variable_name, unit_code, datetime, value, qualifier
            ))
    conn.commit()

def fetch_noaa_data():
    # For now just read a file with a years worth of data
    # Load the JSON data from the file
    with open('LaConnerNOAA.json', 'r') as file:
        data = json.load(file)
    return data

def process_and_store_noaa_data(data):
    timestamp = datetime.now()
    # Insert data into the PostgreSQL table
    # Insert data into the table
    insert_query = """
    INSERT INTO noaa_tide_predictions (prediction_time, value, type)
    VALUES (%s, %s, %s);
    """

    for prediction in data['predictions']:
        prediction_time = datetime.strptime(prediction['t'], '%Y-%m-%d %H:%M')
        value = float(prediction['v'])
        tide_type = prediction['type']

        cursor.execute(insert_query, (prediction_time, value, tide_type))

    # Commit the transaction and close the connection
    conn.commit()
    print(f"NOAA data stored successfully at {timestamp}")

def process_and_store_hydro_view_data(data):
    timestamp = datetime.now()
    # Insert data into the PostgreSQL table
    for location in data:
        location_id = location['locationId']
        for parameter in location['parameters']:
            parameter_id = parameter['parameterId']
            unit_id = parameter['unitId']
            custom_parameter = parameter['customParameter']
            for reading in parameter['readings']:
                timestamp = reading['timestamp']
                value = reading['value']
                cursor.execute("""
                    INSERT INTO locations (location_id, parameter_id, unit_id, custom_parameter, timestamp, value)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (location_id, parameter_id, timestamp) DO NOTHING;
                """, (location_id, parameter_id, unit_id, custom_parameter, timestamp, value))

    # Commit the transaction and close the connection
    conn.commit()
    print(f"Data stored successfully at {timestamp}")

if __name__ == "__main__":
    # tail -f /var/log/cron.log
    import os
    from dotenv import load_dotenv
    from datetime import datetime, timezone, timedelta


    # Load the .env file
    load_dotenv()

    # Access environment variables
    token_url = os.getenv("TOKEN_URL")
    hv_data_url = os.getenv("DATA_URL")
    usgs_data_url = os.getenv("USGS_DATA_URL")
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    location_id = os.getenv("LOCATION_ID")
    location_id1 = os.getenv("LOCATION_ID1")
    hv_default_start_time = os.getenv("HV_DEFAULT_START_TIME") 

    # Print to verify (optional)
    print(f"Token URL: {token_url}")
    print(f"Data URL: {hv_data_url}")
    print(f"Client ID: {client_id}")
    print(f"Client Secret: {client_secret}")
    print(f"Location ID: {location_id}")
    print(f"Location ID: {location_id1}")
    print("-----------------------------------")

    access_token = get_hydro_view_access_token(client_id, client_secret, token_url)
    all_data = fetch_hydro_view_data(hv_data_url, hv_default_start_time, location_id, access_token)
    all_data1 = fetch_hydro_view_data(hv_data_url, hv_default_start_time, location_id1, access_token)

    if(len(all_data)):
        process_and_store_hydro_view_data(all_data)
    if(len(all_data1)):
        process_and_store_hydro_view_data(all_data1)

    timestamp = datetime.now()
    print(f"Access token {access_token} retrieved at {timestamp}") 

    usgs_data = fetch_usgs_data(usgs_data_url, hv_default_start_time)
    if(len(usgs_data)):
        process_and_store_usgs_data(usgs_data)

    if 0: # got data until 08/31/2025 in attached json file
        noaa_data = fetch_noaa_data()
        if(len(noaa_data)):
            process_and_store_noaa_data(noaa_data)

    conn.commit()
    cursor.close()
    conn.close()
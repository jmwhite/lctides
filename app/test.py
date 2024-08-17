import requests
import json
from datetime import datetime, timedelta

# Calculate current UTC time and time 4 hours ago
end_time = datetime.utcnow()
start_time = end_time - timedelta(hours=400)

# Format times for the API request
start_time_str = start_time.strftime('%Y-%m-%dT%H:%MZ')  + timedelta(seconds=1)
end_time_str = end_time.strftime('%Y-%m-%dT%H:%MZ')

# URL for the API request
url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites=12181000,12184700,12194000,12200500&parameterCd=00060,00065&siteType=ST&siteStatus=active&startDT={start_time_str}&endDT={end_time_str}"

# Make the request to the USGS API
response = requests.get(url)
data = response.json()

print(data)
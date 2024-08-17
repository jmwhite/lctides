# A Flask/Dash app that reads data from the database and generates the Plotly dashboard.
import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2

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

    return df

# Sample DataFrame with random time series data for demonstration
def create_sample_dataframe():
    dates = pd.date_range(start=datetime.now() - timedelta(days=30), end=datetime.now(), freq='D')
    data = {f'Series_{i}': np.random.randn(len(dates)).cumsum() for i in range(5)}
    df = pd.DataFrame(data, index=dates)
    return df

# Create sample DataFrame
df = create_sample_dataframe()

# Initialize the Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Time Series Dashboard"),
    dcc.Graph(id='time-series-plot', figure={}),
    *[
        dcc.Graph(
            id=f'time-series-plot-{col}',
            figure=px.line(df, x=df.index, y=col, title=f'Time Series: {col}')
        )
        for col in df.columns
    ]
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
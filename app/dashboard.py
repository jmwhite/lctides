# A Flask/Dash app that reads data from the database and generates the Plotly dashboard.
import dash
from dash import dcc, html
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utilities import get_usgs_data_between_dates, get_noaa_tide_predictions, get_data_between_timestamps


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
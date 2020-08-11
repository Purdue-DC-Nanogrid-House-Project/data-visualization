import configparser
import pickle
import pandas as pd
from IPython.core.display import display
from plotly.graph_objs import Layout

config = configparser.ConfigParser()
config.read('config/appconfig.ini')
filename = '2018-03-22_to_2019-09-04_hourly_weather_data.pkl'
file = open(config['DATA_DIRECTORIES']['processed_weather_data_directory'] + '\\' + filename, 'rb')
loaded_data = pickle.load(file)

# Display loaded weather dataframe
display(loaded_data)

# Display temperatures over the time period
import plotly.express as px
fig = px.line(loaded_data, x=loaded_data.time, y=loaded_data.temperature)
fig.update_layout(xaxis_title="Month, Year", yaxis_title="Temperature [F]", title="DC House Historical Weather Data",
                  template="none")
fig.show()

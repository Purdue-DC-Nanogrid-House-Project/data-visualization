# Import packages
import pickle
import pandas as pd
from dateutil import tz
from datetime import datetime, timedelta
from config.appconfig import config

# Local definitions
emcb_heat_pump_name = 'AC_Compressor'
ted_heat_pump_name = 'Outdoor_Unit'
trane_heat_pump_name = 'trane_energy_df'
data_type = '.pkl'

emcb_datetime_col_name = 'Date'
ted_datetime_col_name = 'Time'
trane_datetime_col_name = 'Date'
emcb_data_col_name = 'Data'
ted_data_col_name = 'Data'
trane_data_col_name = 'Data'

balance_point_temperature = 65 # Degrees F

start_date = '2018-03-22'
end_date = '2020-06-02'
hourly_weather_filename = start_date + '_to_' + end_date + '_hourly_weather_data.pkl'
daily_weather_filename = start_date + '_to_' + end_date + '_daily_weather_data.pkl'

ending_date = datetime(2020, 6, 1)


# Load hourly and daily weather data
file = open(config.PROCESSED_WEATHER_DATA_DIR + '\\' + hourly_weather_filename, 'rb')
weather_hourly_df = pickle.load(file)
weather_hourly_df.reset_index(drop=True, inplace=True)

file = open(config.PROCESSED_WEATHER_DATA_DIR + '\\' + daily_weather_filename, 'rb')
weather_daily_df = pickle.load(file)
weather_daily_df.reset_index(drop=True, inplace=True)


# Load heat pump energy data from each source

# Load EMCB Data and standardize datetime
file = open(config.PROCESSED_EMCB_DATA_DIR + '\\' + emcb_heat_pump_name + data_type, 'rb')
emcb_heat_pump_df = pickle.load(file)
emcb_heat_pump_df.reset_index(drop=True, inplace=True)
emcb_heat_pump_df[emcb_datetime_col_name] = pd.to_datetime(emcb_heat_pump_df[emcb_datetime_col_name])
emcb_heat_pump_df.sort_values(by=emcb_datetime_col_name, inplace=True, ascending=True)

# Load TED Data and standardize datetime
file = open(config.PROCESSED_TED_DATA_DIR + '\\' + ted_heat_pump_name + data_type, 'rb')
ted_heat_pump_df = pickle.load(file)
ted_heat_pump_df.reset_index(drop=True, inplace=True)
ted_heat_pump_df[ted_datetime_col_name] = pd.to_datetime(ted_heat_pump_df[ted_datetime_col_name])
ted_heat_pump_df.sort_values(by=ted_datetime_col_name, inplace=True, ascending=True)

# Load Trane Data and standardize datetime
file = open(config.PROCESSED_TRANE_DATA_DIR + '\\' + trane_heat_pump_name + data_type, 'rb')
trane_heat_pump_df = pickle.load(file)
trane_heat_pump_df.reset_index(drop=True, inplace=True)
trane_heat_pump_df[trane_datetime_col_name] = pd.to_datetime(trane_heat_pump_df[trane_datetime_col_name])
trane_heat_pump_df.sort_values(by=trane_datetime_col_name, inplace=True, ascending=True)


# Parse available data to fill in gap period
trane_heat_pump_filtered_df = trane_heat_pump_df[trane_heat_pump_df[trane_datetime_col_name].between(
    emcb_heat_pump_df[emcb_datetime_col_name].iloc[-1], ted_heat_pump_df[ted_datetime_col_name].iloc[0])]

# Trim TED data to end date
ted_heat_pump_filtered_df = ted_heat_pump_df[ted_heat_pump_df[ted_datetime_col_name] <= ending_date]


# Organize weather data by configuration period

# Setup timezone data
from_zone = tz.gettz('UTC')
to_zone = tz.gettz('US/Eastern')

# Define configuration periods
increment_one_day = timedelta(days=1)
baseline_start = datetime(2018, 3, 22).astimezone(to_zone)
baseline_end = datetime(2018, 8, 31).astimezone(to_zone)
cfg1_start = baseline_end + increment_one_day
cfg1_end = datetime(2018, 10, 17).astimezone(to_zone)
cfg2_start = cfg1_end + increment_one_day
cfg2_end = datetime(2019, 8, 21).astimezone(to_zone)
cfg3_start = cfg2_end + increment_one_day
cfg3_end = datetime(2019, 10, 2).astimezone(to_zone)
cfg4_start = cfg3_end + increment_one_day
cfg4_end = datetime(2019, 11, 20).astimezone(to_zone)
cfg5_start = cfg4_end + increment_one_day
cfg5_end = datetime(2020, 6, 1).astimezone(to_zone)

# Extract dataframe data for each period
bl_df = weather_hourly_df[weather_hourly_df.time.between(baseline_start, baseline_end)]
cfg1_df = weather_hourly_df[weather_hourly_df.time.between(cfg1_start, cfg1_end)]
cfg2_df = weather_hourly_df[weather_hourly_df.time.between(cfg2_start, cfg2_end)]
cfg3_df = weather_hourly_df[weather_hourly_df.time.between(cfg3_start, cfg3_end)]
cfg4_df = weather_hourly_df[weather_hourly_df.time.between(cfg4_start, cfg4_end)]
cfg5_df = weather_hourly_df[weather_hourly_df.time.between(cfg5_start, cfg5_end)]
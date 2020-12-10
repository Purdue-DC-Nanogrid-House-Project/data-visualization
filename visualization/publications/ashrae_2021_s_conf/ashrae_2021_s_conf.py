# Import packages
import errno
import pickle
import datetime
import pandas as pd
import numpy as np
import os
import shutil
import math
from os.path import isfile, join
import utilities.definitions as defs
from config.appconfig import config

# Local definitions
data_type = '.pkl'
timestamp_col_name = 'time'
measurement_col_name = 'measurement'
data_value_col_name = 'value'
acre_date_col_name = 'date'
acre_timestamp_col_name = 'time'
acre_PACID_col_name = 'PAC ID'
acre_solar_radiation_col_name = 'Solar Radiation [kW/m^2]'
bin_direction_col_name = 'direction_bin'
bin_speed_start_col_name = 'speed_bin_start'
bin_speed_end_col_name = 'speed_bin_end'
bin_speed_range_col_name = 'speed_range_bin'
bin_count_col_name = 'count'
bin_frequency_col_name = 'frequency'
mph_to_kph = 1.60934


class ASHRAE2021SummerConf:
    def __init__(self):
        self.weather_api_hourly_mask_df = pd.DataFrame()
        self.weather_api_daily_mask_df = pd.DataFrame()
        self.historical_weather_api_hourly_df = pd.DataFrame()
        self.historical_weather_api_daily_df = pd.DataFrame()
        self.local_station_measurements = dict()
        self.acre_station_measurements_df = pd.DataFrame()
        self.illuminance_measurements_df = pd.DataFrame()
        self.binned_wind_df = pd.DataFrame(
            columns=[bin_direction_col_name, bin_speed_range_col_name, bin_count_col_name])

    @staticmethod
    def clear_media_dir():
        # Clear the media directory and regenerate
        try:
            shutil.rmtree(config.ROOT_MEDIA_DIR + config.ASHRAE_2021_S_CONF_IMAGE_DIR, ignore_errors=True)
            os.makedirs(config.ROOT_MEDIA_DIR + config.ASHRAE_2021_S_CONF_IMAGE_DIR)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def load_data(self, start_date, end_date):
        # Load API hourly and daily weather data available
        hourly_data_dir = config.ROOT_DATA_DIR + config.PROCESSED_WEATHER_DATA_DIR + config.HOURLY_DATA
        daily_data_dir = config.ROOT_DATA_DIR + config.PROCESSED_WEATHER_DATA_DIR + config.DAILY_DATA

        hourly_data_files = [f for f in os.listdir(hourly_data_dir) if isfile(join(hourly_data_dir, f))]
        daily_data_files = [f for f in os.listdir(daily_data_dir) if isfile(join(daily_data_dir, f))]

        weather_hourly_df = pd.DataFrame()
        for data_file in hourly_data_files:
            weather_hourly_df = weather_hourly_df.append(self._load_dataframe_from_pickle(hourly_data_dir, data_file))

        weather_daily_df = pd.DataFrame()
        for data_file in daily_data_files:
            weather_daily_df = weather_daily_df.append(self._load_dataframe_from_pickle(daily_data_dir, data_file))

        # Convert wind speed to metric
        weather_hourly_df['windSpeed'] = weather_hourly_df['windSpeed'].mul(mph_to_kph)
        weather_daily_df['windSpeed'] = weather_daily_df['windSpeed'].mul(mph_to_kph)

        # Record all historical data in dataframes
        self.historical_weather_api_hourly_df = weather_hourly_df
        self.historical_weather_api_daily_df = weather_daily_df

        # Slice dataframes based on data needed
        hourly_mask = (weather_hourly_df[timestamp_col_name].dt.tz_localize(None) >= start_date) \
                      & (weather_hourly_df[timestamp_col_name].dt.tz_localize(None) <= end_date)
        daily_mask = (weather_daily_df[timestamp_col_name].dt.tz_localize(None) >= start_date) \
                     & (weather_daily_df[timestamp_col_name].dt.tz_localize(None) <= end_date)

        self.weather_api_hourly_mask_df = weather_hourly_df.loc[hourly_mask]
        self.weather_api_daily_mask_df = weather_daily_df.loc[daily_mask]

        # Load local weather station data
        station_data_dir = config.ROOT_DATA_DIR + config.PROCESSED_STATION_DATA_DIR
        station_data_files = [f for f in os.listdir(station_data_dir) if isfile(join(station_data_dir, f))]

        raw_station_df = pd.DataFrame()
        for data_file in station_data_files:
            # Get data file date
            str_date = data_file[:data_file.find('_')]
            df = pd.read_csv(station_data_dir + data_file, header=None,
                             names=[timestamp_col_name, measurement_col_name, data_value_col_name])
            df[timestamp_col_name] = str_date + ' ' + df[timestamp_col_name]
            df[timestamp_col_name] = pd.to_datetime(df[timestamp_col_name])
            raw_station_df = raw_station_df.append(df)

        # Restructure loaded data
        measurement_dict = dict()
        measurement_names = raw_station_df[measurement_col_name].unique()
        for measurement_name in measurement_names:
            measurement_dict[measurement_name] = (
                raw_station_df[raw_station_df[measurement_col_name] == measurement_name].loc[:,
                [timestamp_col_name, data_value_col_name]].rename(columns={data_value_col_name: measurement_name})
            )
        self.local_station_measurements = measurement_dict

        # Load Purdue/West Lafayette ACRE station data
        acre_data_dir = config.ROOT_DATA_DIR + config.PROCESSED_PURDUE_ACRE_DATA_DIR
        acre_station_df = pd.DataFrame()
        acre_data_files = [f for f in os.listdir(acre_data_dir) if isfile(join(acre_data_dir, f))]
        for data_file in acre_data_files:
            # Get data file date
            df = pd.read_csv(acre_data_dir + data_file, header=[0, 1])
            df.columns = \
                [acre_date_col_name, acre_timestamp_col_name, acre_PACID_col_name, acre_solar_radiation_col_name]
            df[acre_timestamp_col_name] = (df[acre_date_col_name] + ' ' + df[acre_timestamp_col_name])
            df[acre_timestamp_col_name] = pd.to_datetime(df[acre_timestamp_col_name])
            df = df.drop(columns=acre_date_col_name)
            acre_station_df = acre_station_df.append(df)
        self.acre_station_measurements_df = acre_station_df

    @staticmethod
    def _load_dataframe_from_pickle(data_dir, data_file):
        file = open(data_dir + '\\' + data_file, 'rb')
        df = pickle.load(file)
        df.reset_index(drop=True, inplace=True)
        return df

    def calculate_daily_illuminance(self, start_date, end_date):
        delta = datetime.timedelta(days=1)
        while start_date <= end_date:
            daily_mask = (self.local_station_measurements['visibility'][timestamp_col_name].dt.tz_localize(
                None) >= start_date) & (
                                 self.local_station_measurements['visibility'][timestamp_col_name].dt.tz_localize(
                                     None) <= start_date + delta)
            weather_station_daily_df = self.local_station_measurements['visibility'].loc[daily_mask]
            illuminance = \
                np.trapz(y=weather_station_daily_df['visibility'], x=weather_station_daily_df[timestamp_col_name])
            illuminance_df = pd.DataFrame({timestamp_col_name: [start_date], 'Illuminance': [illuminance]})
            self.illuminance_measurements_df = self.illuminance_measurements_df.append(illuminance_df)
            start_date += delta

    @staticmethod
    def convert_integer_to_direction(d):
        dirs = list(map(str, defs.Directions))
        ix = round(d / (360. / len(dirs)))
        return dirs[ix % len(dirs)]

    def bin_wind_data(self, wind_spd_bins):
        # Build the dataframe of histogram values
        df_hist = pd.DataFrame(
            columns=[bin_direction_col_name, bin_speed_start_col_name, bin_speed_end_col_name, bin_count_col_name])
        for index, wind_spd in enumerate(wind_spd_bins):
            try:
                wind_speed_start = int(wind_spd)
                wind_speed_stop = int(wind_spd_bins[index+1])

                for direction in list(map(str, defs.Directions)):
                    df = pd.DataFrame({
                        bin_direction_col_name: [direction],
                        bin_speed_start_col_name: [wind_speed_start],
                        bin_speed_end_col_name: [wind_speed_stop],
                        bin_count_col_name: [0]
                    })
                    df_hist = df_hist.append(df)
            except IndexError:
                pass    # End of iteration reached, continue to the next cycle

        # Rebuild the index for the completed dataframe
        df_hist = df_hist.reset_index(drop=True)

        # Iterate over all rows in the weather api df
        for index, row in self.historical_weather_api_hourly_df.iterrows():
            if math.isnan(row['windBearing']) or math.isnan(row['windSpeed']):
                continue    # This row has invalid data, skip to the next

            # Get the corresponding direction
            direction = self.convert_integer_to_direction(row['windBearing'])

            # Find matching rows in the bin df
            match_spd = ((row['windSpeed'] >= df_hist[bin_speed_start_col_name])
                         & (row['windSpeed'] <= df_hist[bin_speed_end_col_name]))
            match_dir = df_hist[bin_direction_col_name] == direction
            match_df = df_hist[match_spd & match_dir]

            # Extract the index from the matched row
            match_idx = match_df.index.tolist()[0]

            # Increment the count in the binned dataframe
            df_hist.at[match_idx, bin_count_col_name] += 1

        # Modify the dataframe into a form for plotting
        df = df_hist.copy()
        df[bin_speed_start_col_name] = df[bin_speed_start_col_name].astype(str) + '-' + df[
            bin_speed_end_col_name].astype(str)
        df[bin_count_col_name] = df[bin_count_col_name].div(df[bin_count_col_name].max())
        df = df.drop(columns=[bin_speed_end_col_name])
        df = df.rename(columns={bin_speed_start_col_name: bin_speed_range_col_name,
                                bin_count_col_name: bin_frequency_col_name})
        df[bin_frequency_col_name].astype(float)
        self.binned_wind_df = df

    @staticmethod
    def round_to_nearest_int(x, integer_base):
        return integer_base * round(x / integer_base)

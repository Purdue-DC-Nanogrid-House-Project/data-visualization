# Import packages
import pickle
import datetime
import pandas as pd
from os import listdir
from os.path import isfile, join
from config.appconfig import config


# Local definitions
data_type = '.pkl'
timestamp_col_name = 'time'
measurement_col_name = 'measurement'
data_value_col_name = 'value'


class ASHRAE2021SummerConf:
    def __init__(self):
        self.weather_hourly_df = pd.DataFrame()
        self.weather_daily_df = pd.DataFrame()
        self.weather_station_df = pd.DataFrame()
        self.measurement_types = []

    def load_data(self, start_date, end_date):
        # Load hourly and daily weather data available
        hourly_data_dir = config.PROCESSED_WEATHER_DATA_DIR + '\\' + config.HOURLY_DATA
        daily_data_dir = config.PROCESSED_WEATHER_DATA_DIR + '\\' + config.DAILY_DATA

        hourly_data_files = [f for f in listdir(hourly_data_dir)if isfile(join(hourly_data_dir, f))]
        daily_data_files = [f for f in listdir(daily_data_dir) if isfile(join(daily_data_dir, f))]

        weather_hourly_df = pd.DataFrame()
        for data_file in hourly_data_files:
            weather_hourly_df = weather_hourly_df.append(self._load_dataframe_from_pickle(hourly_data_dir, data_file))

        weather_daily_df = pd.DataFrame()
        for data_file in daily_data_files:
            weather_daily_df = weather_daily_df.append(self._load_dataframe_from_pickle(daily_data_dir, data_file))

        # Slice dataframes based on data needed
        hourly_mask = (weather_hourly_df[timestamp_col_name].dt.tz_localize(None) >= start_date) \
            & (weather_hourly_df[timestamp_col_name].dt.tz_localize(None) <= end_date)
        daily_mask = (weather_daily_df[timestamp_col_name].dt.tz_localize(None) >= start_date) \
            & (weather_daily_df[timestamp_col_name].dt.tz_localize(None) <= end_date)

        self.weather_hourly_df = weather_hourly_df.loc[hourly_mask]
        self.weather_daily_df = weather_daily_df.loc[daily_mask]

        # Load local weather station data
        station_data_dir = config.PROCESSED_STATION_DATA_DIR
        station_data_files = [f for f in listdir(station_data_dir) if isfile(join(station_data_dir, f))]

        raw_station_df = pd.DataFrame()
        for data_file in station_data_files:
            # Get data file date
            str_date = data_file[:data_file.find('_')]
            df = pd.read_csv(station_data_dir + '//' + data_file, header=None,
                             names=[timestamp_col_name, measurement_col_name, data_value_col_name])
            df[timestamp_col_name] = str_date + ' ' + df[timestamp_col_name]
            df[timestamp_col_name] = pd.to_datetime(df[timestamp_col_name])
            raw_station_df = raw_station_df.append(df)

        # Restructure loaded data
        measurement_dict = dict()
        measurement_names = raw_station_df[measurement_col_name].unique()
        self.measurement_types = measurement_names
        for measurement_name in measurement_names:
            measurement_dict[measurement_name] = \
                raw_station_df[raw_station_df[measurement_col_name] == measurement_name].loc[
                :, [timestamp_col_name, data_value_col_name]]\
                .rename(columns={data_value_col_name: measurement_name})

            print()



        #self.weather_hourly_df = weather_station_df

    @staticmethod
    def _load_dataframe_from_pickle(data_dir, data_file):
        file = open(data_dir + '\\' + data_file, 'rb')
        df = pickle.load(file)
        df.reset_index(drop=True, inplace=True)
        return df


# Import packages
import pickle
import pandas as pd
from dateutil import tz
from datetime import datetime, date, timedelta
from config.appconfig import config

# Local definitions
emcb_heat_pump_name = 'AC_Compressor'
ted_heat_pump_name = 'Outdoor_Unit'
trane_heat_pump_name = 'trane_energy_df'
data_type = '.pkl'

emcb_datetime_col_name = 'Date'
trane_datetime_col_name = 'Date'
ted_datetime_col_name = 'Time'

emcb_data_col_name = 'Daily Measured Real Energy Consumed [kWh]'
trane_data_col_name = 'Energy [kWh]'
ted_data_col_name = 'Energy [kWh]'
heat_pump_energy_col_name = 'Energy [kWh]'

heating_degree_col_name = 'Heating Degrees [F]'
cooling_degree_col_name = 'Cooling Degrees [F]'
heating_degree_hours_col_name = 'Heating Degree-Hours [F-hr]'
cooling_degree_hours_col_name = 'Cooling Degrees-Hours [F-hr]'
total_degree_hours_col_name = 'Total Degree-Hours [F-hr]'
heating_energy_col_name = 'Heating Energy [kWh]'
cooling_energy_col_name = 'Cooling Energy [kWh]'
energy_per_total_degree_hour_col_name = 'Energy per Total Degree-Hours [kWh/F-hr]'
degree_day_col_name = 'Date'

summary_table_hdh_std_col_name = 'STD HDH/Day [F-hr/day]'
summary_table_hdh_avg_col_name = 'AVG HDH/Day [F-hr/day]'
summary_table_cdh_std_col_name = 'STD CDH/Day [F-hr/day]'
summary_table_cdh_avg_col_name = 'AVG CDH/Day [F-hr/day]'
summary_table_hdh_col_name = 'HDH [F-hr]'
summary_table_cdh_col_name = 'CDH [F-hr]'
summary_table_hp_heat_energy_col_name = 'Heat Pump Heating Energy [kWh]'
summary_table_hp_cool_energy_col_name = 'Heat Pump Cooling Energy [kWh]'
summary_table_hdh_system_effectiveness_col_name = 'System Effectiveness [kWh/F-hr] (HDH)'
summary_table_cdh_system_effectiveness_col_name = 'System Effectiveness [kWh/F-hr] (CDH)'
summary_table_hdh_improvement_col_name = 'HDH %'
summary_table_cdh_improvement_col_name = 'CDH %'

balance_point_temperature = 65  # Degrees F

start_date = '2018-03-22'
end_date = '2020-06-02'
hourly_weather_filename = start_date + '_to_' + end_date + '_hourly_weather_data.pkl'
daily_weather_filename = start_date + '_to_' + end_date + '_daily_weather_data.pkl'

ending_date = datetime(2020, 6, 1)

from_zone = tz.gettz('UTC')
to_zone = tz.gettz('US/Eastern')

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

# Concatenate heat pump energy data
heat_pump_time_arr = pd.concat(
    [emcb_heat_pump_df[emcb_datetime_col_name],
     trane_heat_pump_filtered_df[trane_datetime_col_name],
     ted_heat_pump_filtered_df[ted_datetime_col_name]])

heat_pump_data_arr = pd.concat(
    [emcb_heat_pump_df[emcb_data_col_name],
     trane_heat_pump_filtered_df[trane_data_col_name],
     ted_heat_pump_filtered_df[ted_data_col_name]])

heat_pump_df = pd.concat({'Time': heat_pump_time_arr, heat_pump_energy_col_name: heat_pump_data_arr}, axis=1)
heat_pump_df['Time'] = heat_pump_df['Time'].dt.tz_localize(to_zone)
heat_pump_df.sort_values(by='Time', inplace=True, ascending=True)
heat_pump_df.reset_index(drop=True, inplace=True)

# Organize weather data by configuration period

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

# Bin hourly temperature data corresponding to the balance temperature
heating_degree_df = pd.DataFrame()
cooling_degree_df = pd.DataFrame()
for index, hour_data in weather_hourly_df.iterrows():
    temperature_delta = abs(hour_data['temperature'] - balance_point_temperature)
    if hour_data['temperature'] <= balance_point_temperature:
        heating_degree_df = heating_degree_df.append(
            pd.DataFrame([{'Date': hour_data['time'], heating_degree_col_name: temperature_delta}]))
    else:
        cooling_degree_df = cooling_degree_df.append(
            pd.DataFrame([{'Date': hour_data['time'], cooling_degree_col_name: temperature_delta}]))

# Calculate hourly heat pump energy associated with each heating and cooling degree
heat_pump_degree_day_df = pd.DataFrame()
for index, heat_pump_data in heat_pump_df.iterrows():
    if index == 233:
        print()

    # Calculate the heating degree hours for the day
    heating_degree_data = heating_degree_df[heating_degree_df['Date'].
        between(heat_pump_data['Time'], heat_pump_data['Time'])]
    heating_degree_hours = heating_degree_data[heating_degree_col_name].sum()

    # Calculate the cooling degree hours for the day
    cooling_degree_data = cooling_degree_df[cooling_degree_df['Date'].
        between(heat_pump_data['Time'], heat_pump_data['Time'])]
    cooling_degree_hours = cooling_degree_data[cooling_degree_col_name].sum()

    # Build the heating and cooling degree day dataframe
    total_degree_hours = heating_degree_hours + cooling_degree_hours
    heat_pump_degree_day_df = heat_pump_degree_day_df.append(
        pd.DataFrame([{degree_day_col_name: heat_pump_data['Time'],
                       heating_degree_hours_col_name: heating_degree_hours,
                       cooling_degree_hours_col_name: cooling_degree_hours,
                       total_degree_hours_col_name: total_degree_hours,
                       heat_pump_energy_col_name: heat_pump_data[heat_pump_energy_col_name],
                       heating_energy_col_name: (heating_degree_hours / total_degree_hours)
                                                * heat_pump_data[heat_pump_energy_col_name],
                       cooling_energy_col_name: (cooling_degree_hours / total_degree_hours)
                                                * heat_pump_data[heat_pump_energy_col_name],
                       energy_per_total_degree_hour_col_name:
                           heat_pump_data[heat_pump_energy_col_name] / total_degree_hours
                       }]))

# Clean up the index
heat_pump_degree_day_df.reset_index(drop=True, inplace=True)


# Iterate over each configuration period and assign the associated values
def build_summary_dict(df):
    heating_degree_filter_df = heating_degree_df[heating_degree_df]

    hdh_std = heating_degree_df[heating_degree_df.Date.between(
        df['time'].iloc[0], df['time'].iloc[-1])][heating_degree_col_name].std()
    hdh_avg = heating_degree_df[heating_degree_df.Date.between(
        df['time'].iloc[0], df['time'].iloc[-1])][heating_degree_col_name].mean()
    hdh_total = heating_degree_df[heating_degree_df.Date.between(
        df['time'].iloc[0], df['time'].iloc[-1])][heating_degree_col_name].sum()
    cdh_std = cooling_degree_df[cooling_degree_df.Date.between(
        df['time'].iloc[0], df['time'].iloc[-1])][cooling_degree_col_name].std()
    cdh_avg = cooling_degree_df[cooling_degree_df.Date.between(
        df['time'].iloc[0], df['time'].iloc[-1])][cooling_degree_col_name].mean()
    cdh_total = cooling_degree_df[cooling_degree_df.Date.between(
        df['time'].iloc[0], df['time'].iloc[-1])][cooling_degree_col_name].sum()
    heat_energy = heat_pump_degree_day_df[heat_pump_degree_day_df.Date.between(
        df['time'].iloc[0], df['time'].iloc[-1])][heating_energy_col_name].sum()
    cool_energy = heat_pump_degree_day_df[heat_pump_degree_day_df.Date.between(
        df['time'].iloc[0], df['time'].iloc[-1])][cooling_energy_col_name].sum()

    summary_dict = {
        summary_table_hdh_std_col_name: hdh_std,
        summary_table_hdh_avg_col_name: hdh_avg,
        summary_table_cdh_std_col_name: cdh_std,
        summary_table_cdh_avg_col_name: cdh_avg,
        summary_table_hdh_col_name: hdh_total,
        summary_table_cdh_col_name: cdh_total,
        summary_table_hp_heat_energy_col_name: heat_energy,
        summary_table_hp_cool_energy_col_name: cool_energy,
        summary_table_hdh_system_effectiveness_col_name: heat_energy / hdh_total,
        summary_table_cdh_system_effectiveness_col_name: cool_energy / cdh_total,
        summary_table_hdh_improvement_col_name: 0,
        summary_table_cdh_improvement_col_name: 0
    }
    return summary_dict


bl_summary_dict = build_summary_dict(bl_df)
cfg1_summary_dict = build_summary_dict(cfg1_df)
cfg2_summary_dict = build_summary_dict(cfg2_df)
cfg3_summary_dict = build_summary_dict(cfg3_df)
cfg4_summary_dict = build_summary_dict(cfg4_df)
cfg5_summary_dict = build_summary_dict(cfg5_df)
net_summary_dict = build_summary_dict(weather_hourly_df[weather_hourly_df.time.between(cfg1_start, cfg5_end)])

# Determine percentage improvement
def calculate_improvement(bl_dict, cfg_dict):
    cfg_dict[summary_table_hdh_improvement_col_name] = \
        ((cfg_dict[summary_table_hdh_system_effectiveness_col_name] -
          bl_dict[summary_table_hdh_system_effectiveness_col_name])
         / bl_dict[summary_table_hdh_system_effectiveness_col_name]) * 100.0
    cfg_dict[summary_table_cdh_improvement_col_name] = \
        ((cfg_dict[summary_table_cdh_system_effectiveness_col_name] -
          bl_dict[summary_table_cdh_system_effectiveness_col_name])
         / bl_dict[summary_table_cdh_system_effectiveness_col_name]) * 100.0
    return cfg_dict


cfg1_summary_dict = calculate_improvement(bl_summary_dict, cfg1_summary_dict)
cfg2_summary_dict = calculate_improvement(bl_summary_dict, cfg2_summary_dict)
cfg3_summary_dict = calculate_improvement(bl_summary_dict, cfg3_summary_dict)
cfg4_summary_dict = calculate_improvement(bl_summary_dict, cfg4_summary_dict)
cfg5_summary_dict = calculate_improvement(bl_summary_dict, cfg5_summary_dict)

# Net average improvement
hdh_total = (cfg1_summary_dict[summary_table_hdh_col_name] + cfg2_summary_dict[summary_table_hdh_col_name] +
    cfg3_summary_dict[summary_table_hdh_col_name] + cfg4_summary_dict[summary_table_hdh_col_name] +
    cfg5_summary_dict[summary_table_hdh_col_name])
cdh_total = (cfg1_summary_dict[summary_table_cdh_col_name] + cfg2_summary_dict[summary_table_cdh_col_name] +
    cfg3_summary_dict[summary_table_cdh_col_name] + cfg4_summary_dict[summary_table_cdh_col_name] +
    cfg5_summary_dict[summary_table_cdh_col_name])

net_heat_imp = (cfg1_summary_dict[summary_table_hdh_improvement_col_name]*cfg1_summary_dict[summary_table_hdh_col_name]
                + cfg2_summary_dict[summary_table_hdh_improvement_col_name]*cfg2_summary_dict[summary_table_hdh_col_name]
                + cfg3_summary_dict[summary_table_hdh_improvement_col_name]*cfg3_summary_dict[summary_table_hdh_col_name]
                + cfg4_summary_dict[summary_table_hdh_improvement_col_name]*cfg4_summary_dict[summary_table_hdh_col_name]
                + cfg5_summary_dict[summary_table_hdh_improvement_col_name]*cfg5_summary_dict[
                    summary_table_hdh_col_name]) / hdh_total

net_cool_imp = (cfg1_summary_dict[summary_table_cdh_improvement_col_name]*cfg1_summary_dict[summary_table_cdh_col_name]
                + cfg2_summary_dict[summary_table_cdh_improvement_col_name]*cfg2_summary_dict[summary_table_cdh_col_name]
                + cfg3_summary_dict[summary_table_cdh_improvement_col_name]*cfg3_summary_dict[summary_table_cdh_col_name]
                + cfg4_summary_dict[summary_table_cdh_improvement_col_name]*cfg4_summary_dict[summary_table_cdh_col_name]
                + cfg5_summary_dict[summary_table_cdh_improvement_col_name]*cfg5_summary_dict[
                    summary_table_cdh_col_name]) / cdh_total
from datetime import date
from datetime import timedelta

import pandas as pd
from session_builder.retrive_request_token import create_user_session
from trade_logger.logger import cus_logger

from mw_minisoft.constants.file_constants import *
from mw_minisoft.persistence_operations.account_management import ticks_indi, read_user_info, market_status
from strategy_builder.strategy_builder import *

cus_logger.setLevel(10)


def data_from_url(ticks, interval, from_date, today, kite_session):
    response = None
    data = {"symbol": ticks.replace('_', ':'), "resolution": interval, "date_format": "1", "range_from": from_date,
            "range_to": today, "cont_flag": "0"}
    response = kite_session.history(data)
    if response is None:
        cus_logger.exception(ticks + ' instrument Service not Available')
    if 'error' in response['s']:
        cus_logger.exception(response['message'])
    return response


def download_data_url(ticks, interval, from_date, today, resource_path, kite_session):
    """
     download and modify the result set to fit the technical indicator
     """
    cus_logger.info("started downloading instrument data from dates today %s and from_day %s ", today,
                    from_date)
    try:
        response = data_from_url(ticks, interval, from_date, today, kite_session)
        df = pd.DataFrame(response['candles'], columns=['date', 'open', 'high', 'low', 'close', 'volume'])

        df['date'] = (pd.to_datetime(df['date'], unit='s')).dt.tz_localize('utc').dt.tz_convert('Asia/kolkata')

        df[['true_range', 'average_true_range_period_7', 'final_ub', 'final_lb', 'uptrend',
            'super_trend_7_3', 'super_trend_direction_7_3']] = None
        instrument_history_data = df[
            ['date', 'open', 'low', 'high', 'close', 'volume', 'true_range', 'average_true_range_period_7', 'final_ub',
             'final_lb', 'super_trend_7_3', 'super_trend_direction_7_3']]
        write_data_file(ticks, instrument_history_data, resource_path, interval)
    except Exception as exceptionMessage:
        cus_logger.exception(exceptionMessage)


def generate_historical_data(auto_inputs):
    """
    This code will download the historical data for all indicators
    """
    try:
        ticks_indicator_df = ticks_indi()
        user_info_df = read_user_info()
        user_info_df = user_info_df.loc[user_info_df['zerodha_datafeed'] == 'Y']
        kite_session, user_record = create_user_session(user_info_df.loc[0], FIREFOX_DRIVER_PATH)
        for record_position, instrument_record in ticks_indicator_df.iterrows():
            ticks = instrument_record.instrument_trading_symbol.replace(':', '_')
            data_interval = auto_inputs['data_interval'][0]
            if market_status(ticks):
                from_date = (date.today()) - timedelta(days=5)
                to_date = date.today()
                download_data_url(ticks, data_interval, from_date, to_date, TICKS_FOLDER_, kite_session)
            else:
                cus_logger.info('program is running in PROD -  %s Service not Available', ticks)

    except Exception as exceptionMessage:
        raise exceptionMessage


def model_indicator_data_generator(auto_inputs):
    """
    will generate the technical values for inputted instrument data
    """
    ticks_indicator_df = ticks_indi()
    for record_position, indicator_record in ticks_indicator_df.iterrows():
        if market_status(indicator_record.instrument_name):
            instrument_history_data = read_data_file(indicator_record.instrument_trading_symbol.replace(':', '_'),
                                                     TICKS_FOLDER_, auto_inputs['data_interval'][0])

            instrument_history_data = strategy_data_builder(instrument_history_data.copy(), auto_inputs,
                                                            indicator_record.instrument_trading_symbol)

            write_data_file(str(indicator_record.instrument_trading_symbol.replace(':', '_')),
                            instrument_history_data, TICKS_FOLDER_, str(auto_inputs['data_interval'][0]))


def write_data_file(instrument_token, instrument_data, resources_path, interval):
    instrument_data.to_csv(resources_path + str(instrument_token) + '_' + str(interval) + '.csv', index=False)


def read_data_file(instrument_token, resources_path, interval):
    return pd.read_csv(resources_path + str(instrument_token) + '_' + str(interval) + '.csv')

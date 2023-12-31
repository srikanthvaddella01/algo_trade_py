import datetime

import pandas as pd
from session_builder.retrive_request_token import create_user_session
from trade_logger.logger import cus_logger

from mw_minisoft.constants.file_constants import *

cus_logger.setLevel(10)


def read_user_info():
    """
    All user info will read and send as data-frame
    """
    cus_logger.info("stated read user info data from user_info file")
    user_info_excel = pd.read_csv(USER_INPUTS_FILE)
    user_info_excel_data = pd.DataFrame(user_info_excel).astype(str)
    return user_info_excel_data


def write_user_info(user_id, request_token, public_token, access_token):
    """
    generated user session tokens will be stored
    """
    cus_logger.info("storing user token information in the file sys")
    user_records = pd.read_csv(USER_INPUTS_FILE)
    user_records_data = pd.DataFrame(user_records).astype(str)
    for user_record_position, user_info_record in user_records_data.iterrows():
        if user_info_record['user_id'] == user_id:
            user_records_data.at[user_record_position, 'request_token'] = request_token
            user_records_data.at[user_record_position, 'day'] = datetime.date.today().day
            user_records_data.at[user_record_position, 'access_token'] = access_token
    user_records_data['login_pin'].astype(str)
    user_records_data.to_csv(USER_INPUTS_FILE, index=False)
    return user_records_data


def update_auto_inputs(env, minutes, super_trend_period, super_trend_multiplier):
    """
    user input data would be updated on auto_input csv file
   """
    cus_logger.info("updating the user input data into the auto_input.csv file")
    auto_inputs = pd.read_csv(AUTO_INPUTS_FILE)
    auto_inputs = pd.DataFrame(auto_inputs).astype(str)
    auto_inputs.at[0, 'scheduler_minutes'] = 1 * minutes
    auto_inputs.at[0, 'data_interval'] = str(1 * minutes)
    auto_inputs.at[0, 'super_trend_period'] = super_trend_period
    auto_inputs.at[0, 'super_trend_multiplier'] = super_trend_multiplier
    auto_inputs.at[0, 'env'] = env
    auto_inputs.to_csv(AUTO_INPUTS_FILE, index=False)


def download_each_user_tokens():
    """
    This code will be used to obtain the accessToken from the source system, which will then be used to access the
    order API and other services.
    """
    user_info = pd.read_csv(USER_INPUTS_FILE)
    user_info = user_info[user_info.day != datetime.date.today().day]
    for user_record_position, user_record in user_info.iterrows():
        cus_logger.info("user(%s) session token generation started", user_record.user_id)
        user_kite_session, user_record = create_user_session(user_record, FIREFOX_DRIVER_PATH)
        write_user_info(user_record.user_id, user_record.request_token, user_record.public_token,
                        user_record.access_token)
    cus_logger.info("session token generation completed ")


def ticks_indi():
    """
    instruments data will read and send as dataframe
   """
    cus_logger.info("instruments are reading from ticks_ind.csv file")
    ticks_ind_excel = pd.read_csv(TICKS_IND_FILE)
    ticks_ind_excel_data = pd.DataFrame(ticks_ind_excel)
    return ticks_ind_excel_data


def collect_user_id(kite_session):
    user_info = read_user_info()
    user_info_id = (user_info[user_info.api_key == kite_session.client_id]).user_id.values[0]
    return user_info_id


def ticks_ind_collect_instrument(instrument_trading_symbol):
    ticks_ind = pd.read_csv(TICKS_IND_FILE)
    ticks_ind = ticks_ind[ticks_ind.instrument_trading_symbol == instrument_trading_symbol]
    return ticks_ind


def market_status(ticks):
    status = False
    auto_inputs = pd.read_csv(AUTO_INPUTS_FILE)
    auto_inputs = pd.DataFrame(auto_inputs).astype(str)
    if auto_inputs.iloc[0].env != 'test':
        user_info_df = read_user_info()
        user_info_df = user_info_df.loc[user_info_df['zerodha_datafeed'] == 'Y']
        kite_session, user_record = create_user_session(user_info_df.loc[0], FIREFOX_DRIVER_PATH)
        market_status_ = kite_session.market_status()

        for key in market_status_['marketStatus']:
            if ('NSE' in ticks) & ('NIFTY' in ticks):
                if (key['exchange'] == 10) & (key['segment'] == 10) & (key['status'] == 'OPEN'):
                    status = True
                    break
            elif ('NSE' in ticks) & ('INR' in ticks):
                if (key['exchange'] == 10) & (key['segment'] == 12) & (key['status'] == 'OPEN'):
                    status = True
                    break
            elif 'MCX' in ticks:
                if (key['exchange'] == 11) & (key['status'] == 'OPEN'):
                    status = True
                    break
    else:
        status = True
    return status



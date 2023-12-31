import pandas as pd
from trade_logger.logger import cus_logger

from mw_minisoft.constants.file_constants import *

cus_logger.setLevel(10)


def read_instrument_tokens(instrument, strike_price, signal, exchange, expiry_day):
    """
        The 'ce' and 'pe' instruments  from the data.csv file were picked up by this piece of code.
    """

    try:
        cus_logger.info("read_instrument_tokens execution    ---    started")
        date_filter = ((pd.to_datetime(expiry_day)).strftime('%y-%b-%d')).split('-')
        if exchange == 'MCX':
            instruments = pd.read_csv('resources/instruments/mcx_data.csv')
            instruments = instruments[instruments['Scrip code'] == instrument.split(':')[1]]
        else:
            instruments = pd.read_csv(INSTRUMENTS_DATA_FILE)
            instruments = instruments[instruments['Scrip code'] == instrument.split(':')[1]]
            instruments = instruments[instruments['Option type'].isin(['CE', 'PE'])]
            instruments[['script', 'year', 'mon', 'day', 'strike_price', 'option_type_1']] = \
                instruments['Symbol Details'].str.split(' ', expand=True)
            instruments = instruments[(instruments['year'] == date_filter[0]) &
                                      (instruments['mon'] == date_filter[1]) & (instruments['day'] == date_filter[2])]

        ticks_ind = pd.read_csv(TICKS_IND_FILE)
        ticks_ind = ticks_ind[ticks_ind.instrument_name == instrument]

        if signal == 'BUY':
            instruments_df_head = instruments[(instruments['option_type_1'] == 'CE') &
                                              (instruments['Strike price'] >= (
                                                      strike_price + ticks_ind.strike_price_position.values[
                                                  0]))].sort_values(by=['strike_price'], ascending=True)

        else:
            instruments_df_head = instruments[(instruments['option_type_1'] == 'PE') &
                                              (instruments['Strike price'] <= (
                                                      strike_price + (ticks_ind.strike_price_position.values[
                                                  0])))].sort_values(by=['strike_price'], ascending=False)
    except Exception as exceptionMessage:
        cus_logger.exception("read_instrument_tokens execution - failed due to this error message %s",
                             exceptionMessage)

    return instruments_df_head.head(1)


def download_write_instrument_tokens():
    # initialize data of lists.
    cus_logger.info('Downloading instrument tokens Started')
    fyers_instruments_urls = {'segments': ['Currency', 'NSE_Capital', 'NSE_Equity', 'MCX'],
                              'urls': ['https://public.fyers.in/sym_details/NSE_CD.csv',
                                       'https://public.fyers.in/sym_details/NSE_CM.csv',
                                       'https://public.fyers.in/sym_details/NSE_FO.csv',
                                       'https://public.fyers.in/sym_details/MCX_COM.csv']}
    df_fyers_instruments_urls = pd.DataFrame(fyers_instruments_urls)
    df_fyers_instruments = pd.DataFrame()
    try:
        for index, row in df_fyers_instruments_urls.iterrows():
            url_csv_data = pd.read_csv(row.urls)
            url_csv_data = url_csv_data.loc[:, ~url_csv_data.columns.str.contains('^Unnamed')]
            df_url_csv_data = pd.DataFrame(url_csv_data.values,
                                           columns=["Fytoken", "Symbol Details", "Exchange Instrument type",
                                                    "Minimum lot size", "Tick size", "ISIN", "Trading Session",
                                                    "Last update date", "Expiry date", "Symbol ticker",
                                                    "Exchange", "Segment", "Scrip code", "Underlying scrip code",
                                                    "Strike price", "Option type"])
            df_fyers_instruments = df_fyers_instruments.append(df_url_csv_data)
        df_fyers_instruments.to_csv(INSTRUMENTS_DATA_FILE, index=False)
    except Exception as exception:
        cus_logger.exception(exception)
    cus_logger.info('Downloading instrument token had completed')
